package com.siwei.web;

import com.siwei.transfer.PullStore;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.function.Supplier;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

@Service
public class ConsoleOperations {
    private final PullStore db;

    public ConsoleOperations(PullStore db) { this.db = db; }

    public void run(String actor, String object, String action, String nonce, String before, Supplier<String> execute) {
        if (object.length() > 36 || nonce == null || !nonce.matches("[0-9a-f-]{36}"))
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "INVALID_OPERATION");
        String id = UUID.nameUUIDFromBytes((actor + "\n" + object + "\n" + action + "\n" + nonce)
                .getBytes(StandardCharsets.UTF_8)).toString();
        if (db.update("INSERT INTO console_operation(id,actor,object_id,action,before_state,result) "
                + "VALUES (?,?,?,?,?,'REQUESTED') ON CONFLICT DO NOTHING", id, actor, object, action, before) == 0) return;
        try {
            String next = execute.get();
            db.update("UPDATE console_operation SET after_state=?,result='ACCEPTED' WHERE id=?", next, id);
        } catch (RuntimeException error) {
            db.update("UPDATE console_operation SET after_state=?,result='REJECTED' WHERE id=?", before, id);
            throw error;
        }
    }
}
