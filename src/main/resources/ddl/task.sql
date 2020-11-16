create table debezium_to_kudu.task
(
    task_num        int auto_increment
        primary key,
    task_id         varchar(768)                                          not null,
    kudu_table_name varchar(768)                                          not null,
    json            text                                                  not null,
    create_time     datetime                    default CURRENT_TIMESTAMP not null,
    state           enum ('ACTIVE', 'INACTIVE') default 'ACTIVE'          not null,
    worker          varchar(512)                                          null,
    update_time     datetime                                              null,
    constraint task_task_id_uk
        unique (task_id)
);

create index task_kudu_table_name_index
    on debezium_to_kudu.task (kudu_table_name);

