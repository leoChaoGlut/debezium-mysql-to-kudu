create table debezium_to_kudu.history
(
    task_id     varchar(768)                       not null,
    json        text                               not null,
    create_time datetime default CURRENT_TIMESTAMP not null
);

create index history_task_id_index
    on debezium_to_kudu.history (task_id);

