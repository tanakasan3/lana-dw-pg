with recursive
    account_set_members as (

        select distinct account_set_id, member_id, member_type

        from {{ ref("int_account_set_members") }}

    ),

    account_set_members_expanded as (

        select account_set_id, member_id, member_type, ARRAY[account_set_id] as set_hierarchy
        from account_set_members

        union all

        select
            l.account_set_id,
            r.member_id,
            r.member_type,
            l.set_hierarchy || ARRAY[r.account_set_id] as set_hierarchy
        from account_set_members_expanded as l
        left join account_set_members as r on l.member_id = r.account_set_id
        where l.member_type = 'AccountSet'

    ),

    -- Get the longest hierarchy for each (account_set_id, member_id, member_type)
    ranked as (
        select
            account_set_id,
            member_id,
            member_type,
            set_hierarchy,
            row_number() over (
                partition by account_set_id, member_id, member_type 
                order by array_length(set_hierarchy, 1) desc nulls last
            ) as rn
        from account_set_members_expanded
        where member_id is not null
    )

select
    account_set_id,
    member_id,
    member_type,
    set_hierarchy

from ranked

where rn = 1
