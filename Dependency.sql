With Scope as (
	select 1 as Target, 2 as Source
	union all
	select 1 as Target, 3 as Source
	union all
	select 2 as Target, null as Source
	union all
	select 3 as Target, 4 as Source
	union all
	select 5 as Target, 1 as Source
	union all
	select 6 as Target, 4 as Source
	union all
	select 7 as Target, 4 as Source
	union all
	select 6 as Target, 8 as Source
	union all
	select 1 as Target, 8 as Source
	union all
	select 2 as Target, 8 as Source

), RecursiveDependents AS (
    -- Anchor CTE: Start with direct dependencies
    SELECT 
        Source as Idx,
        Target AS Dependent_Idx
    FROM 
        Scope
    WHERE 
        not Source is null
    
    UNION ALL
    
    -- Recursive CTE: Find indirect dependencies
    SELECT 
        r.Idx,
        Target as Idx
    FROM 
        Scope p
    INNER JOIN 
        RecursiveDependents r 
    ON 
        p.Source = r.Dependent_Idx
)
,
RecursiveDependencies AS (
    -- Anchor CTE: Start with direct dependencies
    SELECT 
        Target as Idx,
        Source AS Dependency_Idx
    FROM 
        Scope
    WHERE 
        Source IS NOT NULL
    
    UNION ALL
    
    -- Recursive CTE: Find indirect dependencies
    SELECT 
        r.Idx,
        Source AS Dependency_Idx
    FROM 
        Scope p
    INNER JOIN 
        RecursiveDependencies r 
    ON 
        p.Target = r.Dependency_Idx
	where not Source is null
),
Distinct_RecursiveDependents as 
(
select distinct * from RecursiveDependents
),
Distinct_RecursiveDependencies as 
(
select distinct * from RecursiveDependencies
)
-- Final query to aggregate the results
SELECT 
    p.Target as Id,
    
    -- List of downstream
    ISNULL(
        STUFF((
            SELECT '|[' + cast(rd.Dependent_Idx as varchar) + ']'
            FROM Distinct_RecursiveDependents rd
            WHERE rd.Idx = p.Target
            FOR XML PATH(''), TYPE
        ).value('.', 'NVARCHAR(MAX)'), 1, 1, ''),
        null
    ) AS Dependents,
    
    -- List of upstream
    ISNULL(
        STUFF((
            SELECT '|[' + cast(rdp.Dependency_Idx as varchar) +']'
            FROM Distinct_RecursiveDependencies rdp
            WHERE rdp.Idx = p.Target
            FOR XML PATH(''), TYPE
        ).value('.', 'NVARCHAR(MAX)'), 1, 1, ''),
        null
    ) AS Dependencies

FROM 
    Scope p
GROUP BY 
    p.Target;
