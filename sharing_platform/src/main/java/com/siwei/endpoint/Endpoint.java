package com.siwei.endpoint;

import javax.persistence.*;

@Entity
@Table(name = "endpoint")
public class Endpoint {
    @Id @Column(length = 36) public String id;
    @Column(nullable = false) public long revision;
    @Column(nullable = false) public boolean enabled;
    @Version @Column(name = "lock_version", nullable = false) public long lockVersion;
}
