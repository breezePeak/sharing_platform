package com.siwei.endpoint;

import java.io.IOException;
import java.time.Instant;
import java.util.*;
import javax.validation.Validator;
import com.siwei.protocol.ControlledPaths;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;

@Service
public class EndpointService {
    private final EndpointRepository endpoints;
    private final EndpointRevisionRepository revisions;
    private final EndpointOperationRepository operations;
    private final EndpointSettings settings;
    private final CredentialCipher cipher;
    private final Validator validator;

    public EndpointService(EndpointRepository endpoints, EndpointRevisionRepository revisions,
                           EndpointOperationRepository operations, EndpointSettings settings,
                           CredentialCipher cipher, Validator validator) {
        this.endpoints = endpoints; this.revisions = revisions; this.operations = operations;
        this.settings = settings; this.cipher = cipher; this.validator = validator;
    }

    @Transactional(readOnly = true)
    public List<Endpoint> list() { return endpoints.findAll(org.springframework.data.domain.Sort.by("id")); }

    @Transactional(readOnly = true)
    public EndpointRevision current(String id) {
        Endpoint endpoint = endpoints.findById(id).orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND));
        return revisions.findByEndpointIdAndRevision(id, endpoint.revision).orElseThrow();
    }

    @Transactional(readOnly = true)
    public EndpointForm form(String id) {
        EndpointRevision r = current(id);
        EndpointForm f = form(r);
        f.enabled = endpoints.findById(id).orElseThrow().enabled;
        return f;
    }

    @Transactional
    public String save(String id, EndpointForm f) { return save(id, f, false, "SAVE"); }

    @Transactional
    public void toggle(String id, long revision) {
        EndpointForm f = form(id); f.revision = revision; f.enabled = !f.enabled;
        save(id, f, false, f.enabled ? "ENABLE" : "DISABLE");
    }

    @Transactional
    public void clearCredential(String id, long revision) {
        EndpointForm f = form(id); f.revision = revision;
        save(id, f, true, "CLEAR_CREDENTIAL");
    }

    private String save(String id, EndpointForm f, boolean clear, String action) {
        if (!validator.validate(f).isEmpty()) throw new IllegalArgumentException("连接参数无效");
        Endpoint e = id == null ? new Endpoint() : endpoints.lockById(id)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND));
        if (e.revision != f.revision) throw new ResponseStatusException(HttpStatus.CONFLICT, "配置已更新，请刷新页面");
        EndpointRevision previous = id == null ? null : revisions.findByEndpointIdAndRevision(id, e.revision).orElseThrow();
        if (action.equals("SAVE")) validate(f);
        if (e.id == null) e.id = UUID.randomUUID().toString();
        e.revision++; e.enabled = f.enabled;
        endpoints.saveAndFlush(e);
        EndpointRevision r = new EndpointRevision();
        r.id = UUID.randomUUID().toString(); r.endpointId = e.id; r.revision = e.revision;
        r.name = f.name; r.protocol = f.protocol; r.host = f.host; r.port = f.port; r.username = f.username;
        r.auth = f.auth; r.keyFile = blank(f.keyFile); r.fingerprint = blank(f.fingerprint);
        if (clear) r.keyFile = null;
        if (r.keyFile != null) {
            try {
                r.keyDigest = action.equals("SAVE") ? ControlledPaths.keyDigest(ControlledPaths.keyFile(settings.getKeyDirectory(), r.keyFile)) : previous.keyDigest;
            } catch (IOException exception) { throw new IllegalArgumentException("私钥文件不可用或超出大小限制"); }
        }
        r.readRoot = ControlledPaths.remoteRoot(f.readRoot); r.writeRoot = ControlledPaths.remoteRoot(f.writeRoot);
        r.readTestFile = blank(f.readTestFile); r.probeDirectory = blank(f.probeDirectory);
        r.localSourceRef = blank(f.localSourceRef); r.localTargetRef = blank(f.localTargetRef);
        r.timeoutSeconds = f.timeoutSeconds; r.concurrency = f.concurrency;
        r.passive = f.passive; r.isolatedFtpRoot = f.isolatedFtpRoot; r.createdAt = Instant.now();
        String password = f.password == null ? "" : f.password;
        if (!clear) {
            if (!password.isEmpty()) r.secret = cipher.encrypt(password, r.id);
            else if (previous != null && previous.auth == f.auth && previous.protocol == f.protocol && previous.hasSecret()) {
                r.secret = cipher.encrypt(cipher.decrypt(previous), r.id);
            }
            if (r.auth == EndpointRevision.Auth.PASSWORD && !r.hasSecret()
                    && (previous == null || previous.hasSecret() || previous.auth != r.auth)) {
                throw new IllegalArgumentException("密码认证需要凭据；留空仅保留同类认证的原凭据");
            }
        }
        revisions.saveAndFlush(r);
        operations.save(new EndpointOperation(e, action));
        return e.id;
    }

    private void validate(EndpointForm f) {
        String read = ControlledPaths.remoteRoot(f.readRoot), write = ControlledPaths.remoteRoot(f.writeRoot);
        if (read == null && write == null) throw new IllegalArgumentException("至少配置一个远端读写根目录");
        if (blank(f.readTestFile) != null) ControlledPaths.resolve(read, f.readTestFile);
        if (blank(f.probeDirectory) != null) ControlledPaths.resolve(write, f.probeDirectory);
        if (f.protocol == EndpointRevision.Protocol.FTP) {
            if (f.password != null && f.password.codePoints().anyMatch(c -> c < 32 || c == 127))
                throw new IllegalArgumentException("FTP 密码不能含控制字符");
            if (f.auth != EndpointRevision.Auth.PASSWORD || blank(f.keyFile) != null || blank(f.fingerprint) != null)
                throw new IllegalArgumentException("FTP 仅支持密码认证，不能填写 SSH 参数");
            if (!f.isolatedFtpRoot) throw new IllegalArgumentException("FTP 需管理员确认账号根目录隔离，路径链接验证能力有限");
        } else {
            if (f.fingerprint == null || !f.fingerprint.matches("SHA256:[A-Za-z0-9+/]{43}"))
                throw new IllegalArgumentException("SFTP 必须预配可信 SHA256 主机指纹");
            if (f.auth == EndpointRevision.Auth.PASSWORD && blank(f.keyFile) != null)
                throw new IllegalArgumentException("密码认证不能引用私钥");
            if (f.auth == EndpointRevision.Auth.KEY) {
                try { ControlledPaths.keyFile(settings.getKeyDirectory(), f.keyFile); }
                catch (IOException e) { throw new IllegalArgumentException("私钥必须引用管理员受限目录内可读的普通文件"); }
            }
        }
        for (String ref : new String[] {f.localSourceRef, f.localTargetRef}) {
            if (blank(ref) == null) continue;
            String path = settings.getLocalRoots().get(ref);
            if (path == null) throw new IllegalArgumentException("本地根目录引用未由管理员配置");
            try { ControlledPaths.localDirectory(path); }
            catch (IOException e) { throw new IllegalArgumentException("本地根目录不可用"); }
        }
    }

    private static String blank(String value) { return value == null || value.isBlank() ? null : value; }

    private static EndpointForm form(EndpointRevision r) {
        EndpointForm f = new EndpointForm();
        f.revision = r.revision; f.name = r.name; f.protocol = r.protocol; f.host = r.host;
        f.port = r.port; f.username = r.username; f.auth = r.auth; f.keyFile = r.keyFile;
        f.fingerprint = r.fingerprint; f.readRoot = r.readRoot; f.writeRoot = r.writeRoot;
        f.readTestFile = r.readTestFile; f.probeDirectory = r.probeDirectory;
        f.localSourceRef = r.localSourceRef; f.localTargetRef = r.localTargetRef;
        f.timeoutSeconds = r.timeoutSeconds; f.concurrency = r.concurrency;
        f.passive = r.passive; f.isolatedFtpRoot = r.isolatedFtpRoot;
        return f;
    }
}
