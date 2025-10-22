insert into param_staff_por_classe (classe_aeronave, asg, aux_lider, asa_rampa, asa_triagem, asa_desemb, oper_equip)
values
    ('C208', 0, 0, 0, 0, 0, 1),
    ('ATR', 2, 1, 2, 1, 1, 1),
    ('NARROW', 3, 1, 3, 1, 1, 1),
    ('A321', 3, 1, 5, 1, 1, 1),
    ('WIDE', 10, 1, 4, 1, 1, 2),
    ('CARGO', 0, 1, 3, 1, 1, 4),
    ('GOL_MELI', 0, 1, 6, 0, 0, 2)
on conflict (classe_aeronave) do update
set
    asg = excluded.asg,
    aux_lider = excluded.aux_lider,
    asa_rampa = excluded.asa_rampa,
    asa_triagem = excluded.asa_triagem,
    asa_desemb = excluded.asa_desemb,
    oper_equip = excluded.oper_equip;

