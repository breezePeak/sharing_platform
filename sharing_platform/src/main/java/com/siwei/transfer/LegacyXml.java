package com.siwei.transfer;

import java.io.StringReader;
import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilderFactory;
import org.w3c.dom.Element;
import org.xml.sax.InputSource;
import org.xml.sax.SAXParseException;

/** Provisional XML shape; disabled until a customer sample and ACK contract are confirmed. */
final class LegacyXml {
    private LegacyXml() { }

    static String notification(String notificationId, Task task, String stage, TaskProcessor.Result result) {
        StringBuilder xml = new StringBuilder("<").append(stage.equals("PRECHECK") ? "doAsk" : "doComplete")
                .append("><notificationId>").append(escape(notificationId)).append("</notificationId><taskRecordId>")
                .append(escape(task.taskRecordId)).append("</taskRecordId><taskId>").append(escape(task.taskId)).append("</taskId>");
        if (stage.equals("PRECHECK")) {
            xml.append("<result>").append(result.status.equals("WAITING_FEEDBACK") ? "0" : "1").append("</result>");
            if (result.errorDetail != null) xml.append("<rejectrason>").append(escape(result.errorDetail)).append("</rejectrason>");
        } else {
            xml.append("<status>").append(result.status.equals("SUCCEEDED") ? "0" : "1").append("</status>")
                    .append("<fileCount>").append(task.products.size()).append("</fileCount><ProductList>");
            for (java.util.Map<String, Object> product : task.products) {
                xml.append("<product><productId>").append(escape((String) product.get("product_id")))
                        .append("</productId><productPath>").append(escape(task.deliveryPath(product)))
                        .append("</productPath>");
                if (product.get("product_name") != null)
                    xml.append("<productName>").append(escape((String) product.get("product_name"))).append("</productName>");
                xml.append("</product>");
            }
            xml.append("</ProductList>");
        }
        if (result.errorType != null) xml.append("<errorType>").append(escape(result.errorType)).append("</errorType>");
        if (!stage.equals("PRECHECK") && result.errorDetail != null)
            xml.append("<errorDetail>").append(escape(result.errorDetail)).append("</errorDetail>");
        return xml.append("</").append(stage.equals("PRECHECK") ? "doAsk" : "doComplete").append('>').toString();
    }

    static boolean ack(String raw, String notificationId, String taskRecordId) {
        if (raw == null || raw.length() > 4096) return false;
        try {
            Element root = parse(raw);
            return root.getTagName().equals("ack") && value(root, "accepted").equals("true")
                    && value(root, "notificationId").equals(notificationId)
                    && value(root, "taskRecordId").equals(taskRecordId);
        } catch (Exception e) { return false; }
    }

    static Element parse(String raw) throws Exception {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
        factory.setFeature("http://xml.org/sax/features/external-general-entities", false);
        factory.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
        factory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
        factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_DTD, "");
        factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_SCHEMA, "");
        factory.setExpandEntityReferences(false); factory.setXIncludeAware(false);
        var builder = factory.newDocumentBuilder();
        builder.setErrorHandler(new org.xml.sax.helpers.DefaultHandler() {
            @Override public void fatalError(SAXParseException error) throws SAXParseException { throw error; }
        });
        return builder.parse(new InputSource(new StringReader(raw))).getDocumentElement();
    }

    private static String value(Element root, String name) {
        var nodes = root.getElementsByTagName(name);
        if (nodes.getLength() != 1 || nodes.item(0).getParentNode() != root) return "";
        return nodes.item(0).getTextContent();
    }

    private static String escape(String value) {
        return value.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
                .replace("\"", "&quot;").replace("'", "&apos;");
    }
}
