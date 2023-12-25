merge into gold.dim_account gdc
using (select * from silver.subscriptions where is_valid=True) ss
on gdc.id = ss.id 
when matched then
    update set 
        join_date = ss.join_date,
        age = ss.age,
        gender = ss.gender,
        country = ss.country,
        modified_date = CURRENT_TIMESTAMP
    when not matched then
        insert (id, join_date, age, gender, country)
        values (ss.id, ss.join_date, ss.age, ss.gender, ss.country);