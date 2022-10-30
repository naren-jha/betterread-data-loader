package com.njha.betterreaddataloader.author;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import org.springframework.data.annotation.Id;
import org.springframework.data.cassandra.core.cql.PrimaryKeyType;
import org.springframework.data.cassandra.core.mapping.CassandraType;
import org.springframework.data.cassandra.core.mapping.Column;
import org.springframework.data.cassandra.core.mapping.PrimaryKeyColumn;
import org.springframework.data.cassandra.core.mapping.Table;

/**
 * This class represents information about book authors. Authors are inserted in our system first
 * because the data dump that we are going to use for inserting books, has only author id and not their name.
 * So we are first going to insert the mapping of author id to author object in our system, so that
 * while inserting books, we can get author name from author id (because with books, we'd like to
 * keep author name as well. so that we don't have to fetch author names separately when displaying book
 * details. We would want to be able to query everything that is needed on book detail page in one query
 * from Cassandra - that's the way you store data in Cassandra - you denormalize data as much as needed
 * for faster access to information; that's the purpose of using Cassandra - provide faster access while operating on big data)
 */
@Table(value = "author_by_id")
@Setter
@Getter
@Builder
public class Author {

    @Id @PrimaryKeyColumn(name = "author_id", ordinal = 0, type = PrimaryKeyType.PARTITIONED)
    private String id;

    @Column("author_name")
    @CassandraType(type = CassandraType.Name.TEXT)
    private String name;

    @Column("personal_name")
    @CassandraType(type = CassandraType.Name.TEXT)
    private String personalName;
}
