WITH exp_table_sony as (
			SELECT
			p.Title
			, p.Distributor
			, CASE WHEN s.Name LIKE '%CANAL%' THEN 'CanalVOD'ELSE s.Name END AS store
			,p.Nationality
			, LEFT(p.ReleaseWeek,4) as Release_Year
			,c.WeekId
			,p.ReleaseWeek as EST_VOD_RleaseWeek
			,SUM(CASE WHEN p.VODEST = 'EST' THEN r.SalesValue2 ELSE 0 END) as SalesValEST
			,SUM(CASE WHEN p.VODEST = 'VOD' THEN r.SalesValue2 ELSE 0 END) as SalesValVOD
			,SUM(CASE WHEN p.VODEST = 'EST' THEN r.SalesVolume2 ELSE 0 END) as SalesVolEST
			,SUM(CASE WHEN p.VODEST = 'VOD' THEN r.SalesVolume2 ELSE 0 END) as SalesVolVOD
			FROM Product p
			JOIN RawData r ON p.ProductId = r.ProductId and p.VODEST in ('VOD','EST')
			JOIN Source s ON s.SourceId = r.SourceId
			JOIN Calendar c ON c.DayId = r.DayId
			WHERE Distributor ='SPHE' and r.SourceId IN (1,2,3,5,6,8,9,10,11,14,16,18,19, 13) and DisplayCategory= 'Film' AND r.CountryId = 1  AND r.DayId >= 20210101 AND c.WeekId > 20210101 AND LEFT(p.ReleaseWeek,4) >='2021'
			GROUP BY
			p.Title,
			p.Distributor,
			s.Name,
			p.Nationality,
			c.WeekId,
			p.ReleaseWeek

			UNION ALL

			SELECT
			p.Title
			, p.Distributor
			, 'ALL' as store
			,p.Nationality
			, LEFT(p.ReleaseWeek,4) as Release_Year
			,c.WeekId
			,p.ReleaseWeek as EST_VOD_RleaseWeek
			,SUM(CASE WHEN p.VODEST = 'EST' THEN r.SalesValue2 ELSE 0 END) as SalesValEST
			,SUM(CASE WHEN p.VODEST = 'VOD' THEN r.SalesValue2 ELSE 0 END) as SalesValVOD
			,SUM(CASE WHEN p.VODEST = 'EST' THEN r.SalesVolume2 ELSE 0 END) as SalesVolEST
			,SUM(CASE WHEN p.VODEST = 'VOD' THEN r.SalesVolume2 ELSE 0 END) as SalesVolVOD
			FROM Product p
			JOIN RawData r ON p.ProductId = r.ProductId  and p.VODEST in ('VOD','EST')
			JOIN Source s ON s.SourceId = r.SourceId
			JOIN Calendar c ON c.DayId = r.DayId
			WHERE p.DisplayCategory= 'Film' AND  r.CountryId = 1  AND  c.WeekId > 20210101 AND LEFT(p.ReleaseWeek,4) >='2021'
			and ((r.SourceId IN (1,2,3,5,6,8,9,10,11,14,16,18,19, 13) AND p.Distributor in ('DISNEY') AND (r.DayId >= 20210101 ))
			 OR
			 (r.SourceId IN (1,2,3,5,6,8,9,10,11,14,16,18,19, 13) AND p.Distributor in ('FOX','GAUMONT','PATHE','M6SND','STUDIOCANAL','TF1','UNIVERSAL','WARNER','HBO')AND (r.DayId >= 20210101 ))
			 OR
			 (r.sourceId IN (1,5,6,8, 9) AND p.Distributor in ('PARAMOUNT') AND (r.DayId >= 20210101 ))
			 OR
			 (r.sourceId IN (3, 10,13,16,18) AND p.Distributor not in ('FOX','GAUMONT','PATHE','M6SND','STUDIOCANAL','TF1','SPHE','UNIVERSAL','WARNER','HBO','DISNEY','OUT') AND (r.DayId >= 20210101 )))
			GROUP BY
			p.Title,
			p.Distributor,
			s.Name,
			p.Nationality,
			c.WeekId,
			p.ReleaseWeek
			),
share_table_final as (SELECT
    Title,
    Distributor,
    store,
    Nationality,
    Release_Year,
    WeekId,
    EST_VOD_RleaseWeek,
    SUM(SalesVolEST) as SalesVolEST,
    SUM(SalesVolVOD) as SalesVolVOD,
	SUM(SalesValEST) as SalesValEST,
	SUM(SalesValVOD) as SalesValVOD,
    COALESCE(SUM(SalesVolEST) / NULLIF(SUM(SalesVolEST) + SUM(SalesVolVOD), 0), 0)*100 AS SHARE_EST_VOL,
	COALESCE(SUM(SalesVolVOD) / NULLIF(SUM(SalesVolEST) + SUM(SalesVolVOD), 0), 0)*100 AS SHARE_VOD_VOL,
	COALESCE(SUM(SalesValEST) / NULLIF(SUM(SalesValEST) + SUM(SalesValVOD), 0), 0)*100 AS SHARE_EST_VAL,
	COALESCE(SUM(SalesValVOD) / NULLIF(SUM(SalesValEST) + SUM(SalesValVOD), 0), 0)*100 AS SHARE_VOD_VAL
FROM
    exp_table_sony
GROUP BY
    Title,
    Distributor,
    store,
    Nationality,
    Release_Year,
    WeekId,
    EST_VOD_RleaseWeek),


amazon_id_table AS (
 SELECT distinct Title, max(IMDBCode) as IMDBCode FROM Product where  IMDBCode <> '' group by Title
),


exp_table_sony_id_amzl AS (
	SELECT  NULLIF(amzl.IMDBCode,0) AS AmazonCode,
	sonexp.*
	from share_table_final sonexp left JOIN amazon_id_table amzl ON sonexp.Title = amzl.Title
	),

table_genre as (
SELECT DISTINCT ISNULL(Title,'') as Title ,Genre
FROM Product p, RawData r, Calendar c  WHERE
r.SourceId=10
AND p.ProductId=r.ProductId
AND MS_LicenseType<>''
AND c.DayId=r.DayId
AND r.CountryId=1
AND r.DayId > 20210101
),

exp_table_sony_genre AS (
	SELECT tb.Genre AS Genre, sonamzl.*  from exp_table_sony_id_amzl sonamzl left JOIN table_genre tb ON sonamzl.Title = tb.Title
	),
table_subfinal as (
			select
					Genre
					,Title
					,AmazonCode
					,Distributor
					,store
					,Nationality
					,Release_Year
					,WeekId
					,EST_VOD_RleaseWeek
					,SalesVolEST
					,SalesVolVOD
					,SalesValEST
					,SalesValVOD
					,SHARE_EST_VOL
					,SHARE_VOD_VOL
					,SHARE_EST_VAL
					,SHARE_VOD_VAL

			from exp_table_sony_genre
			),

CumulativeData AS (
    SELECT
        Title
        ,WeekId
        ,EST_VOD_RleaseWeek
		,sum(CASE WHEN(TRY_CAST(LEFT(WeekId, 4) AS INT) - TRY_CAST(LEFT(EST_VOD_RleaseWeek, 4) AS INT)) * 52 + TRY_CAST(RIGHT(WeekId, 2) AS INT) - TRY_CAST(RIGHT(EST_VOD_RleaseWeek, 2) AS INT)
		BETWEEN 0 AND 8
            THEN SalesVolEST ELSE 0 END ) AS Sales_vol_EST_8WK
		,sum(CASE WHEN(TRY_CAST(LEFT(WeekId, 4) AS INT) - TRY_CAST(LEFT(EST_VOD_RleaseWeek, 4) AS INT)) * 52 + TRY_CAST(RIGHT(WeekId, 2) AS INT) - TRY_CAST(RIGHT(EST_VOD_RleaseWeek, 2) AS INT)
		BETWEEN 0 AND 8
            THEN SalesVolVOD ELSE 0 END ) AS Sales_vol_VOD_8WK
		,sum(CASE WHEN(TRY_CAST(LEFT(WeekId, 4) AS INT) - TRY_CAST(LEFT(EST_VOD_RleaseWeek, 4) AS INT)) * 52 + TRY_CAST(RIGHT(WeekId, 2) AS INT) - TRY_CAST(RIGHT(EST_VOD_RleaseWeek, 2) AS INT)
		BETWEEN 0 AND 8
            THEN SalesValEST ELSE 0 END ) AS Sales_val_EST_8WK
		,sum(CASE WHEN(TRY_CAST(LEFT(WeekId, 4) AS INT) - TRY_CAST(LEFT(EST_VOD_RleaseWeek, 4) AS INT)) * 52 + TRY_CAST(RIGHT(WeekId, 2) AS INT) - TRY_CAST(RIGHT(EST_VOD_RleaseWeek, 2) AS INT)
		BETWEEN 0 AND 8
            THEN SalesValVOD ELSE 0 END ) AS Sales_val_VOD_8WK
			---- 26wk
		,sum(CASE WHEN(TRY_CAST(LEFT(WeekId, 4) AS INT) - TRY_CAST(LEFT(EST_VOD_RleaseWeek, 4) AS INT)) * 52 + TRY_CAST(RIGHT(WeekId, 2) AS INT) - TRY_CAST(RIGHT(EST_VOD_RleaseWeek, 2) AS INT)
		BETWEEN 0 AND 26
            THEN SalesVolEST ELSE 0 END ) AS Sales_vol_EST_26WK
		,sum(CASE WHEN(TRY_CAST(LEFT(WeekId, 4) AS INT) - TRY_CAST(LEFT(EST_VOD_RleaseWeek, 4) AS INT)) * 52 + TRY_CAST(RIGHT(WeekId, 2) AS INT) - TRY_CAST(RIGHT(EST_VOD_RleaseWeek, 2) AS INT)
		BETWEEN 0 AND 26
            THEN SalesVolVOD ELSE 0 END ) AS Sales_vol_VOD_26WK
		,sum(CASE WHEN(TRY_CAST(LEFT(WeekId, 4) AS INT) - TRY_CAST(LEFT(EST_VOD_RleaseWeek, 4) AS INT)) * 52 + TRY_CAST(RIGHT(WeekId, 2) AS INT) - TRY_CAST(RIGHT(EST_VOD_RleaseWeek, 2) AS INT)
		BETWEEN 0 AND 26
            THEN SalesValEST ELSE 0 END ) AS Sales_val_EST_26WK
		,sum(CASE WHEN(TRY_CAST(LEFT(WeekId, 4) AS INT) - TRY_CAST(LEFT(EST_VOD_RleaseWeek, 4) AS INT)) * 52 + TRY_CAST(RIGHT(WeekId, 2) AS INT) - TRY_CAST(RIGHT(EST_VOD_RleaseWeek, 2) AS INT)
		BETWEEN 0 AND 26
            THEN SalesValVOD ELSE 0 END ) AS Sales_val_VOD_26WK
			---- 52wk
		,sum(CASE WHEN(TRY_CAST(LEFT(WeekId, 4) AS INT) - TRY_CAST(LEFT(EST_VOD_RleaseWeek, 4) AS INT)) * 52 + TRY_CAST(RIGHT(WeekId, 2) AS INT) - TRY_CAST(RIGHT(EST_VOD_RleaseWeek, 2) AS INT)
		BETWEEN 0 AND 52
            THEN SalesVolEST ELSE 0 END ) AS Sales_vol_EST_52WK
		,sum(CASE WHEN(TRY_CAST(LEFT(WeekId, 4) AS INT) - TRY_CAST(LEFT(EST_VOD_RleaseWeek, 4) AS INT)) * 52 + TRY_CAST(RIGHT(WeekId, 2) AS INT) - TRY_CAST(RIGHT(EST_VOD_RleaseWeek, 2) AS INT)
		BETWEEN 0 AND 52
            THEN SalesVolVOD ELSE 0 END ) AS Sales_vol_VOD_52WK
		,sum(CASE WHEN(TRY_CAST(LEFT(WeekId, 4) AS INT) - TRY_CAST(LEFT(EST_VOD_RleaseWeek, 4) AS INT)) * 52 + TRY_CAST(RIGHT(WeekId, 2) AS INT) - TRY_CAST(RIGHT(EST_VOD_RleaseWeek, 2) AS INT)
		BETWEEN 0 AND 52
            THEN SalesValEST ELSE 0 END ) AS Sales_val_EST_52WK
		,sum(CASE WHEN(TRY_CAST(LEFT(WeekId, 4) AS INT) - TRY_CAST(LEFT(EST_VOD_RleaseWeek, 4) AS INT)) * 52 + TRY_CAST(RIGHT(WeekId, 2) AS INT) - TRY_CAST(RIGHT(EST_VOD_RleaseWeek, 2) AS INT)
		BETWEEN 0 AND 52
            THEN SalesValVOD ELSE 0 END ) AS Sales_val_VOD_52WK

    FROM table_subfinal
	group by
        Title,
        WeekId,
        EST_VOD_RleaseWeek
),

calcul_cumul_Nwk as (
select Title
	,sum(Sales_vol_EST_8WK) as Sales_vol_EST_8WK
	,sum(Sales_vol_VOD_8WK) as Sales_vol_VOD_8WK
	,sum(Sales_val_EST_8WK) as Sales_val_EST_8WK
	,sum(Sales_val_VOD_8WK) as Sales_val_VOD_8WK
	,sum(Sales_vol_EST_26WK)as Sales_vol_EST_26WK
	,sum(Sales_vol_VOD_26WK)as Sales_vol_VOD_26WK
	,sum(Sales_val_EST_26WK)as Sales_val_EST_26WK
	,sum(Sales_val_VOD_26WK)as Sales_val_VOD_26WK
	,sum(Sales_vol_EST_52WK)as Sales_vol_EST_52WK
	,sum(Sales_vol_VOD_52WK)as Sales_vol_VOD_52WK
	,sum(Sales_val_EST_52WK)as Sales_val_EST_52WK
	,sum(Sales_val_VOD_52WK)as Sales_val_VOD_52WK

from CumulativeData
group by Title
)

SELECT TOP(1000) * from table_subfinal t1 LEFT JOIN calcul_cumul_Nwk t2 ON t1.Title = t2.Title
