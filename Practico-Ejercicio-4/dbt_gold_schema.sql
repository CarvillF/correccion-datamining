WITH joined_tables AS (
    FROM {{ source('megaline', 'users') }} as u
        INNER JOIN {{ source('megaline', 'plans') }} as p 
            ON u.plan = p.plan_name

    -- LEFT JOIN usado debido a la posibilidad de no hacer consumo de alguno de los servicios
        LEFT JOIN {{ ref('stg_calls') }} as c 
            ON u.user_id = c.user_id AND u.month_sk = c.month_sk
        LEFT JOIN {{ ref('stg_messages') }} as m 
            ON u.user_id = m.user_id AND u.month_sk = m.month_sk
        LEFT JOIN {{ ref('stg_internet') }} as i 
            ON u.user_id = i.user_id AND u.month_sk = i.month_sk
)