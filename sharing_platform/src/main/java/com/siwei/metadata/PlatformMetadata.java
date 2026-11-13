package com.siwei.metadata;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Table(name = "platform_metadata")
public class PlatformMetadata {
    @Id
    @Column(name = "name", length = 100, nullable = false)
    private String name;

    @Column(name = "value", length = 500, nullable = false)
    private String value;

    protected PlatformMetadata() { }

    public PlatformMetadata(String name, String value) {
        this.name = name;
        this.value = value;
    }

    public String getName() { return name; }
    public String getValue() { return value; }
}
