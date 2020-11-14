create table instance
(
    instance_num int auto_increment primary key,
    instance_id  varchar(768)                       not null,
    json         text                               not null,
    create_time  datetime default CURRENT_TIMESTAMP not null,

    constraint instance_name_uindex unique (instance_id)
);

