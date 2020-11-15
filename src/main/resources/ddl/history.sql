create table history
(
    task_id     varchar(768)                       not null,
    json        text                               not null,
    create_time datetime default CURRENT_TIMESTAMP not null
);

