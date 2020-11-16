create table debezium_to_kudu.offset
(
    task_id     varchar(768)                       not null,
    `key`       blob                               not null,
    value       blob                               not null,
    create_time datetime default CURRENT_TIMESTAMP not null
);

create index offset_task_id_index
    on debezium_to_kudu.offset (task_id);

