package com.siwei.transfer;

import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.*;
import com.siwei.endpoint.*;
import com.siwei.protocol.ControlledPaths;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.*;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.web.server.ResponseStatusException;
import org.w3c.dom.*;
import org.xml.sax.InputSource;

@Service
public class TaskService {
    private static final Set<String> FIELDS = Set.of("taskId", "direction", "endpointId", "sourceCode", "sourceRoot", "sourcePath", "targetPath", "products");
    private final ObjectMapper json;
    private final JdbcTemplate db;
    private final TransactionTemplate tx;
    private final TransferSettings settings;
    private final EndpointRepository endpoints;
    private final EndpointRevisionRepository revisions;
    private final List<TaskProcessor> processors;

    public TaskService(ObjectMapper json, JdbcTemplate db, org.springframework.transaction.PlatformTransactionManager transactionManager,
                       TransferSettings settings, EndpointRepository endpoints, EndpointRevisionRepository revisions,
                       List<TaskProcessor> processors) {
        this.json = json; this.db = db; this.tx = new TransactionTemplate(transactionManager);
        this.settings = settings; this.endpoints = endpoints; this.revisions = revisions; this.processors = processors;
    }

    public String authenticate(String authorization) {
        if (authorization == null || !authorization.startsWith("Bearer ")) throw error(HttpStatus.UNAUTHORIZED, "AUTH_REQUIRED");
        byte[] actual = authorization.substring(7).getBytes(StandardCharsets.UTF_8);
        for (Map.Entry<String, TransferSettings.Caller> entry : settings.getCallers().entrySet()) {
            String expected = entry.getValue().getToken();
            if (expected != null && !expected.isBlank() && MessageDigest.isEqual(expected.getBytes(StandardCharsets.UTF_8), actual))
                return entry.getKey();
        }
        throw error(HttpStatus.UNAUTHORIZED, "AUTH_REQUIRED");
    }

    public ObjectNode parseJson(String raw) {
        limit(raw);
        try {
            JsonNode node = json.readTree(raw);
            if (!node.isObject()) throw invalid();
            return (ObjectNode) node;
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) { throw invalid(); }
    }

    public ObjectNode parseXml(String raw) {
        limit(raw);
        try {
            Element root = LegacyXml.parse(raw);
            if (!root.getTagName().equals("taskNotice")) throw invalid();
            onlyChildren(root, Set.of("taskId", "type", "sourceRoot", "targetPath", "fileCount", "ProductList"));
            ObjectNode node = json.createObjectNode();
            node.put("taskId", child(root, "taskId")); node.put("direction", "PUSH");
            node.put("type", child(root, "type")); node.put("sourceRoot", child(root, "sourceRoot"));
            node.put("targetPath", child(root, "targetPath"));
            ArrayNode products = node.putArray("products");
            NodeList lists = root.getElementsByTagName("ProductList");
            if (lists.getLength() != 1 || lists.item(0).getParentNode() != root) throw invalid();
            NodeList children = lists.item(0).getChildNodes();
            for (int i = 0; i < children.getLength(); i++) if (children.item(i) instanceof Element) {
                Element product = (Element) children.item(i);
                if (!product.getTagName().equals("product")) throw invalid();
                onlyChildren(product, Set.of("productId", "productPath", "productName"));
                ObjectNode item = products.addObject();
                item.put("productId", child(product, "productId"));
                item.put("productPath", child(product, "productPath"));
                String name = child(product, "productName");
                if (!name.isBlank()) item.put("productName", name);
            }
            if (!Integer.toString(products.size()).equals(child(root, "fileCount"))) throw invalid();
            return node;
        } catch (ResponseStatusException e) { throw e; }
        catch (Exception e) { throw invalid(); }
    }

    private static String child(Element parent, String name) {
        NodeList matches = parent.getElementsByTagName(name);
        if (matches.getLength() != 1 || matches.item(0).getParentNode() != parent) throw invalid();
        return matches.item(0).getTextContent().trim();
    }

    private static void onlyChildren(Element parent, Set<String> allowed) {
        NodeList children = parent.getChildNodes();
        Set<String> seen = new HashSet<>();
        for (int i = 0; i < children.getLength(); i++) if (children.item(i) instanceof Element) {
            Element child = (Element) children.item(i);
            if (!allowed.contains(child.getTagName()) || !seen.add(child.getTagName())) throw invalid();
        }
    }

    public Accepted accept(String caller, ObjectNode input, String raw, String entry) {
        limit(raw);
        TransferSettings.Caller config = settings.getCallers().get(caller);
        String taskId = required(input, "taskId", 100);
        if (!taskId.matches("[A-Za-z0-9][A-Za-z0-9._-]{0,99}")) throw invalid();
        String direction = required(input, "direction", 4);
        if (!direction.equals("PUSH") && !direction.equals("PULL")) throw invalid();
        for (Iterator<String> fields = input.fieldNames(); fields.hasNext();) {
            String field = fields.next();
            if (!FIELDS.contains(field) && !(entry.equals("XML") && field.equals("type"))) throw invalid();
        }
        Task previous = findByExternal(caller, taskId);
        String endpointId = entry.equals("XML")
                ? previous == null ? config.getXmlRoutes().get(required(input, "type", 100)) : previous.endpointId
                : required(input, "endpointId", 36);
        if (endpointId == null) throw error(HttpStatus.BAD_REQUEST, "ROUTE_NOT_CONFIGURED");
        if (!config.getEndpointIds().contains(endpointId)) throw error(HttpStatus.FORBIDDEN, "ENDPOINT_FORBIDDEN");
        String canonical = canonical(input, endpointId);
        if (previous != null) return compare(previous, canonical);
        if (processors.stream().noneMatch(p -> p.direction().equals(direction))) throw error(HttpStatus.SERVICE_UNAVAILABLE, "ENGINE_NOT_READY");
        if (config.getNotificationUrl() == null || config.getNotificationUrl().isBlank())
            throw error(HttpStatus.SERVICE_UNAVAILABLE, "NOTIFICATION_NOT_CONFIGURED");
        java.net.URI notificationUrl;
        try { notificationUrl = java.net.URI.create(config.getNotificationUrl()); }
        catch (IllegalArgumentException e) { throw error(HttpStatus.SERVICE_UNAVAILABLE, "NOTIFICATION_NOT_CONFIGURED"); }
        if (!Set.of("http", "https").contains(notificationUrl.getScheme()) || notificationUrl.getHost() == null
                || notificationUrl.getUserInfo() != null || notificationUrl.getFragment() != null)
            throw error(HttpStatus.SERVICE_UNAVAILABLE, "NOTIFICATION_NOT_CONFIGURED");
        java.sql.Timestamp now = java.sql.Timestamp.from(Instant.now()); String id = UUID.randomUUID().toString();
        int inserted = tx.execute(status -> {
            Endpoint endpoint = endpoints.lockById(endpointId).orElseThrow(() -> error(HttpStatus.BAD_REQUEST, "ENDPOINT_NOT_FOUND"));
            if (!endpoint.enabled) throw error(HttpStatus.BAD_REQUEST, "ENDPOINT_DISABLED");
            EndpointRevision revision = revisions.findByEndpointIdAndRevision(endpointId, endpoint.revision).orElseThrow();
            try { validateScope(input, direction, revision); }
            catch (IllegalArgumentException e) { throw invalid(); }
            int count = db.update("INSERT INTO transfer_task (id,caller_id,external_task_id,entry_type,direction,endpoint_id,endpoint_revision_id,notification_url,canonical_request,raw_request,status,phase,next_run_at,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,'QUEUED','ACCEPTED',?,?,?) ON CONFLICT (caller_id,external_task_id) DO NOTHING",
                    id, caller, taskId, entry, direction, endpointId, revision.id, notificationUrl.toString(), canonical, canonical, now, now, now);
            if (count == 1 && input.has("products")) {
                JsonNode products = input.get("products");
                for (int i = 0; i < products.size(); i++) {
                    JsonNode product = products.get(i);
                    db.update("INSERT INTO transfer_product (task_id,position,product_id,product_path,product_name) VALUES (?,?,?,?,?)",
                            id, i, product.get("productId").asText(), product.get("productPath").asText(),
                            product.has("productName") ? product.get("productName").asText() : null);
                }
            }
            return count;
        });
        if (inserted == 0) {
            Task winner = findByExternal(caller, taskId);
            if (winner == null) throw error(HttpStatus.SERVICE_UNAVAILABLE, "INTAKE_RETRY");
            return compare(winner, canonical);
        }
        return new Accepted(id, taskId, true, false, "QUEUED");
    }

    private Accepted compare(Task task, String canonical) {
        if (!task.canonicalRequest.equals(canonical)) throw error(HttpStatus.CONFLICT, "TASK_ID_CONFLICT");
        return new Accepted(task.taskRecordId, task.taskId, true, true, task.executionStatus);
    }

    private String canonical(ObjectNode input, String endpointId) {
        ObjectNode value = json.createObjectNode();
        value.put("direction", required(input, "direction", 4)); value.put("endpointId", endpointId);
        if (input.hasNonNull("sourceCode") || input.hasNonNull("type")) {
            String code = required(input, input.has("type") ? "type" : "sourceCode", 100);
            if (!code.matches("[A-Za-z0-9][A-Za-z0-9._-]{0,99}")) throw invalid();
            value.put("sourceCode", code);
        }
        for (String field : List.of("sourceRoot", "sourcePath", "targetPath")) {
            JsonNode node = input.get(field);
            if (node != null && !node.isNull()) value.put(field, required(input, field, 1000));
        }
        JsonNode products = input.get("products");
        if (products != null) {
            if (!products.isArray() || products.size() > 1000) throw invalid();
            ArrayNode normalized = value.putArray("products");
            List<ObjectNode> sorted = new ArrayList<>();
            Set<String> productIds = new HashSet<>();
            for (JsonNode product : products) {
                if (!product.isObject() || product.size() > 3) throw invalid();
                ObjectNode p = json.createObjectNode();
                String productId = required(product, "productId", 100);
                if (!productIds.add(productId)) throw invalid();
                p.put("productId", productId);
                p.put("productPath", required(product, "productPath", 1000));
                if (product.hasNonNull("productName")) p.put("productName", required(product, "productName", 200));
                sorted.add(p);
            }
            sorted.sort(Comparator.comparing(p -> p.get("productId").asText()));
            sorted.forEach(normalized::add);
        }
        return value.toString();
    }

    private void validateScope(ObjectNode input, String direction, EndpointRevision revision) {
        if (direction.equals("PUSH")) {
            if (revision.writeRoot == null || revision.localSourceRef == null) throw invalid();
            if (!required(input, "sourceRoot", 100).equals(revision.localSourceRef)) throw invalid();
            ControlledPaths.resolve(revision.writeRoot, required(input, "targetPath", 1000));
            JsonNode products = input.get("products");
            if (products == null || !products.isArray() || products.isEmpty()) throw invalid();
            for (JsonNode product : products) {
                String id = required(product, "productId", 100);
                if (!id.matches("[A-Za-z0-9][A-Za-z0-9._-]{0,99}")) throw invalid();
                ControlledPaths.relative(required(product, "productPath", 1000));
            }
        } else {
            if (revision.readRoot == null || revision.localTargetRef == null) throw invalid();
            ControlledPaths.resolve(revision.readRoot, required(input, "sourcePath", 1000));
            if (!required(input, "targetPath", 100).equals(revision.localTargetRef)) throw invalid();
            if (input.has("products") || input.has("sourceRoot")) throw invalid();
        }
    }

    public Task get(String caller, String id) {
        Task task = read("SELECT * FROM transfer_task WHERE id=? AND caller_id=?", id, caller);
        if (task == null) throw error(HttpStatus.NOT_FOUND, "TASK_NOT_FOUND");
        task.products = db.queryForList("SELECT position,product_id,product_path,product_name FROM transfer_product WHERE task_id=? ORDER BY position", id);
        task.notifications = db.queryForList("SELECT id,stage,status,attempts,last_error FROM transfer_notification WHERE task_id=? ORDER BY created_at", id);
        return task;
    }

    public Map<String, Object> files(String caller, String id, int page, int size) {
        get(caller, id);
        if (page < 0 || size < 1 || size > 100) throw invalid();
        return Map.of("taskRecordId", id, "page", page, "size", size, "manifestFrozen", false,
                "totalFilesKnown", false, "files", List.of());
    }

    private Task findByExternal(String caller, String id) {
        return read("SELECT * FROM transfer_task WHERE caller_id=? AND external_task_id=?", caller, id);
    }
    private Task read(String sql, Object... args) {
        try { return db.queryForObject(sql, (rs, row) -> {
            Task t = new Task(); t.taskRecordId = rs.getString("id"); t.callerId = rs.getString("caller_id");
            t.taskId = rs.getString("external_task_id"); t.entryType = rs.getString("entry_type");
            t.direction = rs.getString("direction"); t.endpointId = rs.getString("endpoint_id");
            t.endpointRevisionId = rs.getString("endpoint_revision_id"); t.canonicalRequest = rs.getString("canonical_request");
            t.executionStatus = rs.getString("status"); t.phase = rs.getString("phase");
            t.acceptanceStatus = rs.getString("acceptance_status"); t.errorType = rs.getString("error_type");
            t.errorDetail = rs.getString("error_detail"); t.createdAt = rs.getTimestamp("created_at").toInstant();
            t.updatedAt = rs.getTimestamp("updated_at").toInstant(); return t;
        }, args); } catch (EmptyResultDataAccessException e) { return null; }
    }

    static String required(JsonNode node, String field, int max) {
        JsonNode value = node.get(field);
        if (value == null || !value.isTextual() || value.asText().isBlank() || value.asText().length() > max) throw invalid();
        return value.asText();
    }
    private static void limit(String raw) { if (raw == null || raw.getBytes(StandardCharsets.UTF_8).length > 32768) throw invalid(); }
    private static ResponseStatusException invalid() { return error(HttpStatus.BAD_REQUEST, "INVALID_TASK"); }
    static ResponseStatusException error(HttpStatus status, String code) { return new ResponseStatusException(status, code); }

    public static class Accepted {
        public final String taskRecordId, taskId, executionStatus;
        public final boolean accepted, reused;
        Accepted(String taskRecordId, String taskId, boolean accepted, boolean reused, String executionStatus) {
            this.taskRecordId = taskRecordId; this.taskId = taskId; this.accepted = accepted;
            this.reused = reused; this.executionStatus = executionStatus;
        }
    }
}
