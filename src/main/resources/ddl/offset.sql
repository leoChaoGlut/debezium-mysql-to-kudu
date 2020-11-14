create table offset
(
    instance_id varchar(768)                       not null,
    `key`       blob                               not null,
    value       blob                               not null,
    create_time datetime default CURRENT_TIMESTAMP not null
);
