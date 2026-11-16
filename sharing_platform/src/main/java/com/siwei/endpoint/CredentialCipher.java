package com.siwei.endpoint;

import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.Base64;
import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import org.springframework.stereotype.Component;

@Component
public class CredentialCipher {
    private final EndpointSettings settings;
    public CredentialCipher(EndpointSettings settings) { this.settings = settings; }

    public String encrypt(String value, String revisionId) {
        byte[] nonce = new byte[12]; new SecureRandom().nextBytes(nonce);
        try {
            Cipher cipher = cipher(Cipher.ENCRYPT_MODE, nonce, revisionId);
            return Base64.getEncoder().encodeToString(nonce) + ":"
                    + Base64.getEncoder().encodeToString(cipher.doFinal(value.getBytes(StandardCharsets.UTF_8)));
        } catch (GeneralSecurityException e) { throw new IllegalStateException("凭据加密失败"); }
    }

    public String decrypt(EndpointRevision revision) {
        if (revision.secret == null) return "";
        try {
            String[] parts = revision.secret.split(":", -1);
            return new String(cipher(Cipher.DECRYPT_MODE, Base64.getDecoder().decode(parts[0]), revision.id)
                    .doFinal(Base64.getDecoder().decode(parts[1])), StandardCharsets.UTF_8);
        } catch (GeneralSecurityException | IllegalArgumentException | IndexOutOfBoundsException e) {
            throw new IllegalStateException("凭据解密失败，请核对外置主密钥");
        }
    }

    private Cipher cipher(int mode, byte[] nonce, String revisionId) throws GeneralSecurityException {
        byte[] key;
        try { key = Base64.getDecoder().decode(settings.getMasterKey()); }
        catch (IllegalArgumentException e) { throw new IllegalArgumentException("必须配置 32 字节 Base64 外置主密钥"); }
        if (key.length != 32) throw new IllegalArgumentException("必须配置 32 字节 Base64 外置主密钥");
        Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
        cipher.init(mode, new SecretKeySpec(key, "AES"), new GCMParameterSpec(128, nonce));
        cipher.updateAAD(revisionId.getBytes(StandardCharsets.UTF_8));
        return cipher;
    }
}
