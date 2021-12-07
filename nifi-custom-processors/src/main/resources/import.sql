create table nifi_log (
    unique_id bigint,
    process_group_name varchar(255),
    process_group_id varchar(255),
    processor_name varchar(255),
    processor_id varchar(255),
    log_order int,
    name varchar(255),
    status varchar(255),
    message varchar(255),
    custom varchar(255),

    constraint pk_composite primary key(unique_id, process_group_id, name)
)