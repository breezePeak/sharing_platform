package com.siwei.protocol;

import java.io.IOException;
import com.siwei.endpoint.EndpointRevision;

abstract class ScopedRemoteClient implements RemoteFileClient {
    protected final EndpointRevision config;
    ScopedRemoteClient(EndpointRevision config) { this.config = config; }

    protected void authorize(String path, boolean write, boolean existing) throws IOException {
        String normalized = ControlledPaths.remoteRoot(path);
        if (normalized == null || !normalized.equals(path)
                || !(write ? ControlledPaths.within(config.writeRoot, path)
                : ControlledPaths.within(config.readRoot, path) || ControlledPaths.within(config.writeRoot, path))) {
            throw new IOException("PATH_OUTSIDE_ROOT");
        }
        String checked = existing ? path : path.substring(0, Math.max(1, path.lastIndexOf('/')));
        checkAncestors(checked);
    }

    protected abstract void checkAncestors(String path) throws IOException;

    protected void temporary(String path) throws IOException {
        String base = config.probeDirectory == null ? null : ControlledPaths.resolve(config.writeRoot, config.probeDirectory);
        String suffix = base != null && path.startsWith(base + "/") ? path.substring(base.length() + 1) : "";
        if (!suffix.matches("\\.sharing-probe-[a-f0-9]{32}(/(payload|renamed))?")) throw new IOException("NOT_PLATFORM_TEMPORARY");
    }
}
