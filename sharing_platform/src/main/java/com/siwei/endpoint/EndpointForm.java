package com.siwei.endpoint;

import javax.validation.constraints.*;
import com.siwei.endpoint.EndpointRevision.*;

/** Only this allowlisted object is bound to HTML; it never holds saved secrets. */
public class EndpointForm {
    public long revision;
    @NotBlank @Size(max = 100) public String name;
    @NotNull public Protocol protocol = Protocol.FTP;
    @NotBlank @Pattern(regexp = "[a-zA-Z0-9][a-zA-Z0-9.:-]{0,252}") public String host;
    @Min(1) @Max(65535) public int port = 21;
    @NotBlank @Size(max = 100) @Pattern(regexp = "[^\\p{Cntrl}]+") public String username;
    @NotNull public Auth auth = Auth.PASSWORD;
    @Size(max = 4096) public String password = "";
    @Size(max = 100) public String keyFile;
    @Size(max = 100) public String fingerprint;
    @Size(max = 1000) public String readRoot;
    @Size(max = 1000) public String writeRoot;
    @Size(max = 500) public String readTestFile;
    @Size(max = 500) public String probeDirectory;
    @Size(max = 100) public String localSourceRef;
    @Size(max = 100) public String localTargetRef;
    @Min(1) @Max(30) public int timeoutSeconds = 5;
    @Min(1) @Max(8) public int concurrency = 1;
    public boolean passive = true;
    public boolean isolatedFtpRoot;
    public boolean enabled = true;
}
