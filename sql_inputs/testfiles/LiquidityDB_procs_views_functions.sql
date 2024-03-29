USE [Liquidity]
GO
/****** Object:  UserDefinedFunction [dbo].[GetBucketProfileFunding]    Script Date: 2/11/2023 1:33:40 pm ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[GetBucketProfileFunding]
(
	@timeprofile VARCHAR(10)
)
RETURNS VARCHAR(50)
AS
BEGIN

/********************************************************************************************************
 * Return time bucket description for funding profile based on the time profile code
 *
 ********************************************************************************************************
 *
 * @version 1.0
 * @author	Stephen Chin
 * @date	2020.08.13
 * 
 * - Start version control
 * - Add new time bucket code of '50Y' to 'Over 1 Year'
 * - Add new time bucket description '1 Month to 6 Months' and '6 Months to 1 Year' by spliting '1 Month to 1 Year' into two
 * 
*/
	RETURN 
	CASE WHEN @timeprofile IN ('0D', '1D') THEN 'Out to 1 Day'
		 WHEN @timeprofile = '2D' THEN '1 Day to 2 Days'
		 WHEN @timeprofile = '3D' THEN '2 Days to 3 Days'
		 WHEN @timeprofile = '4D' THEN '3 Days to 4 Days'
		 WHEN @timeprofile = '5D' THEN '4 Days to 5 Days'
		 WHEN @timeprofile = '6D' THEN '5 Days to 6 Days'
		 WHEN @timeprofile = '1W' THEN '6 Days to 1 Week'
		 WHEN @timeprofile IN ('2W', '3W', '1M') THEN '1 Week to 1 Month'
		 WHEN @timeprofile IN ('2M', '3M', '4M', '5M', '6M') THEN '1 Month to 6 Months'
		 WHEN @timeprofile IN ('1Y') THEN '6 Months to 1 Year'
		 WHEN @timeprofile IN ('18M', '2Y', '3Y', '4Y', '5Y', '7Y', '10Y', '15Y', '20Y', '50Y') THEN 'Over 1 Year'
		 ELSE 'ERROR: New time profile code observed.'
	END; 

END

GO
/****** Object:  UserDefinedFunction [dbo].[GetBucketProfileFundingRBNZ]    Script Date: 2/11/2023 1:33:40 pm ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[GetBucketProfileFundingRBNZ]
(
	@timeprofile VARCHAR(10)
)
RETURNS VARCHAR(50)
AS
BEGIN

/********************************************************************************************************
 * Return time bucket description for funding profile in RBNZ template based on the time profile code
 * 
 ********************************************************************************************************
 *
 * @version 1.0
 * @author	Stephen Chin
 * @date	2020.08.13
 * 
 * - Start version control
 * - Add new time bucket code of '50Y' to 'Over 10 Years'
 *
 * 
*/
	RETURN 
	CASE WHEN @timeprofile IN ('0D', '1D') THEN 'Overnight'
		 WHEN @timeprofile IN ('2D', '3D', '4D', '5D', '6D', '1W') THEN '2 Days to 1 Week'
		 WHEN @timeprofile IN ('2W') THEN '1 Week to 2 Weeks'
		 WHEN @timeprofile IN ('3W', '1M') THEN '2 Weeks to 1 Month'
		 WHEN @timeprofile IN ('2M') THEN '1 Month to 2 Months'
		 WHEN @timeprofile IN ('3M') THEN '2 Months to 3 Months'
		 WHEN @timeprofile IN ('4M', '5M', '6M') THEN '3 Months to 6 Months'
		 WHEN @timeprofile IN ('1Y') THEN '6 Months to 1 Year'
		 WHEN @timeprofile IN ('18M') THEN '1 Year to 18 Months'
		 WHEN @timeprofile IN ('2Y') THEN '18 Months to 2 Years'
		 WHEN @timeprofile IN ('3Y') THEN '2 Years to 3 Years'
		 WHEN @timeprofile IN ('4Y') THEN '3 Years to 4 Years'
		 WHEN @timeprofile IN ('5Y') THEN '4 Years to 5 Years'
		 WHEN @timeprofile IN ('7Y') THEN '5 Years to 7 Years'
		 WHEN @timeprofile IN ('10Y') THEN '7 Years to 10 Years'
		 WHEN @timeprofile IN ('15Y', '20Y', '50Y') THEN 'Over 10 Years'
		 ELSE 'ERROR: New time profile observed.'
	END; 

END

GO
/****** Object:  UserDefinedFunction [dbo].[GetChangeDataCaptureType]    Script Date: 2/11/2023 1:33:40 pm ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


CREATE FUNCTION [dbo].[GetChangeDataCaptureType]
(
	@operation INT
)
RETURNS VARCHAR(50)

AS
BEGIN

/********************************************************************************************************
 * Return CDC operational type in more readable string form
 * 
 *********************************************************************************************************
 *
 * @version 1.0
 * @author	Stephen Chin
 * @date	2020.08.13
 * 
 * - Start version control
 *
 * 
*/
DECLARE @otype VARCHAR(50); 
Set @otype = ''
   
SET @otype = (SELECT CASE WHEN @operation = 1 THEN 'DELETE'
						  WHEN @operation = 2 THEN 'INSERT'
						  WHEN @operation = 3 THEN 'BEFORE UPDATE'
						  WHEN @operation = 4 THEN 'AFTER UPDATE'
						  ELSE 'ERROR' END)

RETURN @otype

END

GO
/****** Object:  UserDefinedFunction [dbo].[GetCommitmentType]    Script Date: 2/11/2023 1:33:40 pm ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


CREATE FUNCTION [dbo].[GetCommitmentType]
(
	@position_date DATETIME, 
	@Date_Loan_Required DATETIME,
	@tolerance INT
)
RETURNS VARCHAR(50)

AS
BEGIN

/********************************************************************************************************
 * Return commitment type
 * 
 ********************************************************************************************************
 *
 * @version 1.0
 * @author	Stephen Chin
 * @date	2020.08.13
 * 
 * - Start version control
 *
 * 
*/
DECLARE @ctype VARCHAR(50); 
Set @ctype = ''
    
SET @ctype = 
   (SELECT CASE WHEN DATEDIFF(d, @position_date, @Date_Loan_Required) < -@tolerance THEN 'OLD COMMITMENT'
			    WHEN DATEDIFF(d, @position_date, @Date_Loan_Required) <= 0 THEN 'PAST DUE CURRENT'
				WHEN DATEDIFF(d, @position_date, @Date_Loan_Required) <= 7 THEN 'DUE WITHIN 1 WEEK'
				WHEN @Date_Loan_Required <= DATEADD(m, 1, @position_date) THEN 'BETWEEN 1 WEEK AND A MONTH'
				WHEN @Date_Loan_Required > DATEADD(m, 1, @position_date) THEN 'LONG COMMITMENT'
				ELSE 'ERROR' END)
                              
RETURN @ctype

END

GO
/****** Object:  UserDefinedFunction [dbo].[GetProductGroup]    Script Date: 2/11/2023 1:33:40 pm ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


CREATE FUNCTION [dbo].[GetProductGroup]
(
	@product VARCHAR(10)
)
RETURNS VARCHAR(10)
AS
BEGIN

/********************************************************************************************************
 * Return product group code by removing sub-product suffix, ".*"
 * 
 ********************************************************************************************************
 *
 * @version 1.0
 * @author	Stephen Chin
 * @date	2020.08.13
 * 
 * - Start version control
 *
 * 
*/
	RETURN CASE WHEN CHARINDEX('.', @product) = 0 THEN @product ELSE SUBSTRING(@product, 1, CHARINDEX('.', @product)-1) END

END

GO
/****** Object:  UserDefinedFunction [dbo].[GetSizeBand]    Script Date: 2/11/2023 1:33:40 pm ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


CREATE FUNCTION [dbo].[GetSizeBand]
(
	@balance DECIMAL(18,2)
)
RETURNS VARCHAR(10)
AS
BEGIN

/********************************************************************************************************
 * Return size band according to BS13 Table 3
 * This allocates a provider of non-market funding to the bank to the size band corresponding to that person’s total assets held at the bank on a given date.
 *
 ********************************************************************************************************
 *
 * @version 1.0
 * @author	Stephen Chin
 * @date	2020.08.13
 * 
 * - Start version control
 * 
*/

RETURN CASE WHEN ABS(@balance) BETWEEN 0 AND 5000000 THEN '0-5mn'
			WHEN ABS(@balance) BETWEEN 5000000 AND 10000000 THEN '5mn-10mn'
			WHEN ABS(@balance) BETWEEN 10000000 AND 20000000 THEN '10mn-20mn'
			WHEN ABS(@balance) BETWEEN 20000000 AND 50000000 THEN '20mn-50mn'
			WHEN ABS(@balance) > 50000000 THEN 'Over 50mn' END

END

GO
/****** Object:  UserDefinedFunction [dbo].[GetTimeProfile]    Script Date: 2/11/2023 1:33:40 pm ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


CREATE FUNCTION [dbo].[GetTimeProfile]
(
	@sdate DATE,
	@days INT
)
RETURNS VARCHAR(10)
AS
BEGIN

/********************************************************************************************************
 * Return time bucket profile code given a start date and number of days
 * Exception: If start date is last day of month, then forward date is also last day of following month.
 * 
 ********************************************************************************************************
 *
 * @version 1.0
 * @author	Stephen Chin
 * @date	2020.08.13
 * 
 * - Start version control
 * - Add new time bucket code of '50Y'
 *
 * 
*/
	RETURN 
	CASE WHEN @sdate = CONVERT(date, DATEADD(SS,-1,DATEADD(MM, DATEDIFF(MM,0,@sdate)+1,0)), 103) THEN
		CASE	WHEN @days <= 0 THEN '0D'
				WHEN @days = 1 THEN '1D'
				WHEN @days = 2 THEN '2D'
				WHEN @days = 3 THEN '3D'
				WHEN @days = 4 THEN '4D'
				WHEN @days = 5 THEN '5D'
				WHEN @days = 6 THEN '6D'				
				WHEN @days <= DATEDIFF(DD, @sdate, DATEADD(WW, 1, @sdate)) THEN '1W'
				WHEN @days <= DATEDIFF(DD, @sdate, DATEADD(WW, 2, @sdate)) THEN '2W'
				WHEN @days <= DATEDIFF(DD, @sdate, DATEADD(WW, 3, @sdate)) THEN '3W'
				WHEN @days <= DATEDIFF(DD, @sdate, DATEADD(SS, -1, DATEADD(MM, DATEDIFF(MM, 0, @sdate)+2, 0))) THEN '1M'
				WHEN @days <= DATEDIFF(DD, @sdate, DATEADD(SS, -1, DATEADD(MM, DATEDIFF(MM, 0, @sdate)+3, 0))) THEN '2M'
				WHEN @days <= DATEDIFF(DD, @sdate, DATEADD(SS, -1, DATEADD(MM, DATEDIFF(MM, 0, @sdate)+4, 0))) THEN '3M'
				WHEN @days <= DATEDIFF(DD, @sdate, DATEADD(SS, -1, DATEADD(MM, DATEDIFF(MM, 0, @sdate)+5, 0))) THEN '4M'
				WHEN @days <= DATEDIFF(DD, @sdate, DATEADD(SS, -1, DATEADD(MM, DATEDIFF(MM, 0, @sdate)+6, 0))) THEN '5M'
				WHEN @days <= DATEDIFF(DD, @sdate, DATEADD(SS, -1, DATEADD(MM, DATEDIFF(MM, 0, @sdate)+7, 0))) THEN '6M'
				WHEN @days <= DATEDIFF(DD, @sdate, DATEADD(SS, -1, DATEADD(MM, DATEDIFF(MM, 0, @sdate)+13, 0))) THEN '1Y'
				WHEN @days <= DATEDIFF(DD, @sdate, DATEADD(SS, -1, DATEADD(MM, DATEDIFF(MM, 0, @sdate)+19, 0))) THEN '18M'
				WHEN @days <= DATEDIFF(DD, @sdate, DATEADD(SS, -1, DATEADD(MM, DATEDIFF(MM, 0, @sdate)+25, 0))) THEN '2Y'
				WHEN @days <= DATEDIFF(DD, @sdate, DATEADD(SS, -1, DATEADD(MM, DATEDIFF(MM, 0, @sdate)+37, 0))) THEN '3Y'
				WHEN @days <= DATEDIFF(DD, @sdate, DATEADD(SS, -1, DATEADD(MM, DATEDIFF(MM, 0, @sdate)+49, 0))) THEN '4Y'
				WHEN @days <= DATEDIFF(DD, @sdate, DATEADD(SS, -1, DATEADD(MM, DATEDIFF(MM, 0, @sdate)+61, 0))) THEN '5Y'
				WHEN @days <= DATEDIFF(DD, @sdate, DATEADD(SS, -1, DATEADD(MM, DATEDIFF(MM, 0, @sdate)+85, 0))) THEN '7Y'
				WHEN @days <= DATEDIFF(DD, @sdate, DATEADD(SS, -1, DATEADD(MM, DATEDIFF(MM, 0, @sdate)+121, 0))) THEN '10Y'
				WHEN @days <= DATEDIFF(DD, @sdate, DATEADD(SS, -1, DATEADD(MM, DATEDIFF(MM, 0, @sdate)+181, 0))) THEN '15Y'
				WHEN @days <= DATEDIFF(DD, @sdate, DATEADD(SS, -1, DATEADD(MM, DATEDIFF(MM, 0, @sdate)+241, 0))) THEN '20Y'
				WHEN @days <= DATEDIFF(DD, @sdate, DATEADD(SS, -1, DATEADD(MM, DATEDIFF(MM, 0, @sdate)+601, 0))) THEN '50Y'
				ELSE '0D'
		END
	ELSE
		CASE	WHEN @days <= 0 THEN '0D'
				WHEN @days = 1 THEN '1D'
				WHEN @days = 2 THEN '2D'
				WHEN @days = 3 THEN '3D'
				WHEN @days = 4 THEN '4D'
				WHEN @days = 5 THEN '5D'
				WHEN @days = 6 THEN '6D'				
				WHEN @days <= DATEDIFF(DD, @sdate, DATEADD(WW, 1, @sdate)) THEN '1W'
				WHEN @days <= DATEDIFF(DD, @sdate, DATEADD(WW, 2, @sdate)) THEN '2W'
				WHEN @days <= DATEDIFF(DD, @sdate, DATEADD(WW, 3, @sdate)) THEN '3W'
				WHEN @days <= DATEDIFF(DD, @sdate, DATEADD(MM, 1, @sdate)) THEN '1M'
				WHEN @days <= DATEDIFF(DD, @sdate, DATEADD(MM, 2, @sdate)) THEN '2M'
				WHEN @days <= DATEDIFF(DD, @sdate, DATEADD(MM, 3, @sdate)) THEN '3M'
				WHEN @days <= DATEDIFF(DD, @sdate, DATEADD(MM, 4, @sdate)) THEN '4M'
				WHEN @days <= DATEDIFF(DD, @sdate, DATEADD(MM, 5, @sdate)) THEN '5M'
				WHEN @days <= DATEDIFF(DD, @sdate, DATEADD(MM, 6, @sdate)) THEN '6M'
				WHEN @days <= DATEDIFF(DD, @sdate, DATEADD(YY, 1, @sdate)) THEN '1Y'
				WHEN @days <= DATEDIFF(DD, @sdate, DATEADD(MM, 18, @sdate)) THEN '18M'
				WHEN @days <= DATEDIFF(DD, @sdate, DATEADD(YY, 2, @sdate)) THEN '2Y'
				WHEN @days <= DATEDIFF(DD, @sdate, DATEADD(YY, 3, @sdate)) THEN '3Y'
				WHEN @days <= DATEDIFF(DD, @sdate, DATEADD(YY, 4, @sdate)) THEN '4Y'
				WHEN @days <= DATEDIFF(DD, @sdate, DATEADD(YY, 5, @sdate)) THEN '5Y'
				WHEN @days <= DATEDIFF(DD, @sdate, DATEADD(YY, 7, @sdate)) THEN '7Y'
				WHEN @days <= DATEDIFF(DD, @sdate, DATEADD(YY, 10, @sdate)) THEN '10Y'
				WHEN @days <= DATEDIFF(DD, @sdate, DATEADD(YY, 15, @sdate)) THEN '15Y'
				WHEN @days <= DATEDIFF(DD, @sdate, DATEADD(YY, 20, @sdate)) THEN '20Y'
				WHEN @days <= DATEDIFF(DD, @sdate, DATEADD(YY, 50, @sdate)) THEN '50Y'
				ELSE '0D'
		END
	END;
	
END

GO
/****** Object:  UserDefinedFunction [dbo].[GetTimeProfileHistRates]    Script Date: 2/11/2023 1:33:40 pm ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[GetTimeProfileHistRates]
(
	@days INT
)
RETURNS VARCHAR(50)
AS
BEGIN

/********************************************************************************************************
 * Return time bucket profile code given a number of days as an input.
 * It is designed for calculating WAC of new issuance in monthly RBNZ template and relative time profiling
 * from a start date is not applicable. 
 * Hence, historical rates table (MRU_Historic_Rates) is used along with original term days.
 * 
 ********************************************************************************************************
 *
 * @version 1.0
 * @author	Stephen Chin
 * @date	2020.08.13
 * 
 * - Start version control
 *
 * 
*/

RETURN 
CASE WHEN @days BETWEEN 0 AND 7 THEN 'Overnight'
	 WHEN @days BETWEEN 8 AND 45 THEN '30'
	 WHEN @days BETWEEN 46 AND 75 THEN '60'
	 WHEN @days BETWEEN 76 AND 95 THEN '90'
	 WHEN @days BETWEEN 96 AND 110 THEN '100'
	 WHEN @days BETWEEN 111 AND 135 THEN '120'
	 WHEN @days BETWEEN 136 AND 165 THEN '150'
	 WHEN @days BETWEEN 166 AND 225 THEN '180'
	 WHEN @days BETWEEN 226 AND 317 THEN '270'
	 WHEN @days BETWEEN 318 AND 456 THEN '365'
	 WHEN @days BETWEEN 457 AND 639 THEN '548'
	 WHEN @days BETWEEN 640 AND 912 THEN '730'
	 WHEN @days BETWEEN 913 AND 1277 THEN '1095'
	 WHEN @days BETWEEN 1278 AND 1642 THEN '1460'
	 WHEN @days BETWEEN 1643 AND 2190 THEN '1825'
	 WHEN @days BETWEEN 2191 AND 3102 THEN '2555'
	 WHEN @days BETWEEN 3103 AND 9999 THEN '3650'
	 ELSE 'Overnight' END;
	 
END

GO
/****** Object:  UserDefinedFunction [dbo].[GetTimeProfileString]    Script Date: 2/11/2023 1:33:40 pm ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[GetTimeProfileString]
(
	@sDate DATE,
	@eDate DATE
)
RETURNS VARCHAR(10)
AS
BEGIN

/********************************************************************************************************
 * Return a time profile string (as defined in TMS Liquidity report) of a given date (@eDate) from a start date (@sDate)
 * The time profile definition method is a period and the grid definition is an end point.
 * 
 ********************************************************************************************************
 *
 * @version 1.0
 * @author	Stephen Chin
 * @date	2021.01.13
 * 
 * - Initial version
 *
 * 
*/

	DECLARE @1d date, @2d date, @3d date, @4d date, @5d date, @6d date;
	DECLARE @1w date, @2w date, @3w date;
	DECLARE @1ws date, @2ws date, @3ws date;
	DECLARE @1m date, @2m date;
	DECLARE @1ms date, @2ms date;
	DECLARE @1q date, @2q date, @3q date;
	DECLARE @1qs date, @2qs date, @3qs date;
	DECLARE @1y date, @2y date, @3y date, @4y date, @5y date, @10y date, @15y date, @20y date, @50y date;
	DECLARE @1ys date, @2ys date, @3ys date, @4ys date, @5ys date, @10ys date, @15ys date, @20ys date, @50ys date;

	SET @1d = DATEADD(DAY, 1, @sDate);
	SET @2d = DATEADD(DAY, 2, @sDate);
	SET @3d = DATEADD(DAY, 3, @sDate);
	SET @4d = DATEADD(DAY, 4, @sDate);
	SET @5d = DATEADD(DAY, 5, @sDate);
	SET @6d = DATEADD(DAY, 6, @sDate);

	SET @1ws = DATEADD(DAY, 1, @6d);
	SET @1w = DATEADD(WEEK, 1, @sDate);
	SET @2ws = DATEADD(DAY, 1, @1w);
	SET @2w = DATEADD(WEEK, 2, @sDate);
	SET @3ws = DATEADD(DAY, 1, @2w);
	SET @3w = DATEADD(WEEK, 3, @sDate);

	SET @1ms = DATEADD(DAY, 1, @3w);
	SET @1m = CASE WHEN @sdate = CONVERT(date, DATEADD(SS,-1,DATEADD(MM, DATEDIFF(MM,0,@sdate)+1,0)), 103) THEN DATEADD(SS, -1, DATEADD(MM, DATEDIFF(MM, 0, @sdate)+2, 0)) ELSE DATEADD(MONTH, 1, @sDate) END;
	SET @2ms = DATEADD(DAY, 1, @1m);
	SET @2m = CASE WHEN @sdate = CONVERT(date, DATEADD(SS,-1,DATEADD(MM, DATEDIFF(MM,0,@sdate)+1,0)), 103) THEN DATEADD(SS, -1, DATEADD(MM, DATEDIFF(MM, 0, @sdate)+3, 0)) ELSE DATEADD(MONTH, 2, @sDate) END;

	SET @1qs = DATEADD(DAY, 1, @2m);
	SET @1q = CASE WHEN @sdate = CONVERT(date, DATEADD(SS,-1,DATEADD(MM, DATEDIFF(MM,0,@sdate)+1,0)), 103) THEN DATEADD(SS, -1, DATEADD(MM, DATEDIFF(MM, 0, @sdate)+4, 0)) ELSE DATEADD(MONTH, 3, @sDate) END;
	SET @2qs = DATEADD(DAY, 1, @1q);
	SET @2q = CASE WHEN @sdate = CONVERT(date, DATEADD(SS,-1,DATEADD(MM, DATEDIFF(MM,0,@sdate)+1,0)), 103) THEN DATEADD(SS, -1, DATEADD(MM, DATEDIFF(MM, 0, @sdate)+7, 0)) ELSE DATEADD(MONTH, 6, @sDate) END;
	SET @3qs = DATEADD(DAY, 1, @2q);
	SET @3q = CASE WHEN @sdate = CONVERT(date, DATEADD(SS,-1,DATEADD(MM, DATEDIFF(MM,0,@sdate)+1,0)), 103) THEN DATEADD(SS, -1, DATEADD(MM, DATEDIFF(MM, 0, @sdate)+10, 0)) ELSE DATEADD(MONTH, 9, @sDate) END;

	SET @1ys = DATEADD(DAY, 1, @3q);
	SET @1y = CASE WHEN @sdate = CONVERT(date, DATEADD(SS,-1,DATEADD(MM, DATEDIFF(MM,0,@sdate)+1,0)), 103) THEN DATEADD(SS, -1, DATEADD(MM, DATEDIFF(MM, 0, @sdate)+13, 0)) ELSE DATEADD(YEAR, 1, @sDate) END;
	SET @2ys = DATEADD(DAY, 1, @1y);
	SET @2y = CASE WHEN @sdate = CONVERT(date, DATEADD(SS,-1,DATEADD(MM, DATEDIFF(MM,0,@sdate)+1,0)), 103) THEN DATEADD(SS, -1, DATEADD(MM, DATEDIFF(MM, 0, @sdate)+25, 0)) ELSE DATEADD(YEAR, 2, @sDate) END;
	SET @3ys = DATEADD(DAY, 1, @2y);
	SET @3y = CASE WHEN @sdate = CONVERT(date, DATEADD(SS,-1,DATEADD(MM, DATEDIFF(MM,0,@sdate)+1,0)), 103) THEN DATEADD(SS, -1, DATEADD(MM, DATEDIFF(MM, 0, @sdate)+37, 0)) ELSE DATEADD(YEAR, 3, @sDate) END;
	SET @4ys = DATEADD(DAY, 1, @3y);
	SET @4y = CASE WHEN @sdate = CONVERT(date, DATEADD(SS,-1,DATEADD(MM, DATEDIFF(MM,0,@sdate)+1,0)), 103) THEN DATEADD(SS, -1, DATEADD(MM, DATEDIFF(MM, 0, @sdate)+49, 0)) ELSE DATEADD(YEAR, 4, @sDate) END;
	SET @5ys = DATEADD(DAY, 1, @4y);
	SET @5y = CASE WHEN @sdate = CONVERT(date, DATEADD(SS,-1,DATEADD(MM, DATEDIFF(MM,0,@sdate)+1,0)), 103) THEN DATEADD(SS, -1, DATEADD(MM, DATEDIFF(MM, 0, @sdate)+61, 0)) ELSE DATEADD(YEAR, 5, @sDate) END;
	SET @10ys = DATEADD(DAY, 1, @5y);
	SET @10y = CASE WHEN @sdate = CONVERT(date, DATEADD(SS,-1,DATEADD(MM, DATEDIFF(MM,0,@sdate)+1,0)), 103) THEN DATEADD(SS, -1, DATEADD(MM, DATEDIFF(MM, 0, @sdate)+121, 0)) ELSE DATEADD(YEAR, 10, @sDate) END;
	SET @15ys = DATEADD(DAY, 1, @10y);
	SET @15y = CASE WHEN @sdate = CONVERT(date, DATEADD(SS,-1,DATEADD(MM, DATEDIFF(MM,0,@sdate)+1,0)), 103) THEN DATEADD(SS, -1, DATEADD(MM, DATEDIFF(MM, 0, @sdate)+181, 0)) ELSE DATEADD(YEAR, 15, @sDate) END;
	SET @20ys = DATEADD(DAY, 1, @15y);
	SET @20y = CASE WHEN @sdate = CONVERT(date, DATEADD(SS,-1,DATEADD(MM, DATEDIFF(MM,0,@sdate)+1,0)), 103) THEN DATEADD(SS, -1, DATEADD(MM, DATEDIFF(MM, 0, @sdate)+241, 0)) ELSE DATEADD(YEAR, 20, @sDate) END;
	SET @50ys = DATEADD(DAY, 1, @20y);
	SET @50y = CASE WHEN @sdate = CONVERT(date, DATEADD(SS,-1,DATEADD(MM, DATEDIFF(MM,0,@sdate)+1,0)), 103) THEN DATEADD(SS, -1, DATEADD(MM, DATEDIFF(MM, 0, @sdate)+601, 0)) ELSE DATEADD(YEAR, 50, @sDate) END;

	RETURN
	 
	CASE WHEN @eDate between @1d and @1d THEN '1 Day' 
		 WHEN @eDate between @2d and @2d THEN '2 Days'
		 WHEN @eDate between @3d and @3d THEN '3 Days'
		 WHEN @eDate between @4d and @4d THEN '4 Days'
		 WHEN @eDate between @5d and @5d THEN '5 Days'
		 WHEN @eDate between @6d and @6d THEN '6 Days'
		 WHEN @eDate between @1ws and @1w THEN '1 Week'
		 WHEN @eDate between @2ws and @2w THEN '2 Weeks'
		 WHEN @eDate between @3ws and @3w THEN '3 Weeks'
		 WHEN @eDate between @1ms and @1m THEN '1 Month'
		 WHEN @eDate between @2ms and @2m THEN '2 Months'
		 WHEN @eDate between @1qs and @1q THEN '1 Quarter'
		 WHEN @eDate between @2qs and @2q THEN '2 Quarters'
		 WHEN @eDate between @3qs and @3q THEN '3 Quarters'
		 WHEN @eDate between @1ys and @1y THEN '1 Year'
		 WHEN @eDate between @2ys and @2y THEN '2 Years'
		 WHEN @eDate between @3ys and @3y THEN '3 Years'
		 WHEN @eDate between @4ys and @4y THEN '4 Years'
		 WHEN @eDate between @5ys and @5y THEN '5 Years'
		 WHEN @eDate between @10ys and @10y THEN '10 Years'
		 WHEN @eDate between @15ys and @15y THEN '15 Years'
		 WHEN @eDate between @20ys and @20y THEN '20 Years'
		 WHEN @eDate between @50ys and @50y THEN '50 Years'
	ELSE
		'ERROR'
	END;

END
	
GO
/****** Object:  UserDefinedFunction [dbo].[InlineMax]    Script Date: 2/11/2023 1:33:40 pm ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


--> Add new functions 
Create Function [dbo].[InlineMax](@val1 float, @val2 float)
returns float
as
begin
  if @val1 > @val2
    return @val1
  return isnull(@val2,@val1)
end

GO
/****** Object:  UserDefinedFunction [dbo].[InlineMin]    Script Date: 2/11/2023 1:33:40 pm ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

Create Function [dbo].[InlineMin](@val1 float, @val2 float)
returns float
as
begin
  if @val1 > @val2
    return @val2
  return isnull(@val1,@val2)
end

GO
/****** Object:  UserDefinedFunction [dbo].[MS_Parse]    Script Date: 2/11/2023 1:33:40 pm ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
Create FUNCTION [dbo].[MS_Parse]
(
@string VARCHAR(8000)
 )
RETURNS VARCHAR(8000)
AS
 BEGIN
 DECLARE @IncorrectCharLoc SMALLINT
SET @IncorrectCharLoc = PATINDEX('%[,()]%', @string)
WHILE @IncorrectCharLoc > 0
BEGIN
 SET @string = STUFF(@string, @IncorrectCharLoc, 1, '')
SET @IncorrectCharLoc = PATINDEX('%[,()]%', @string)
END
 SET @string = @string
RETURN @string
END
GO
/****** Object:  View [dbo].[S8PaymentDetails]    Script Date: 2/11/2023 1:33:40 pm ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE VIEW [dbo].[S8PaymentDetails] AS
SELECT		AccessNo, Product, NextPaymentDate, CAST(Amount AS Money) AS Amount
FROM		Ultracs.AutomaticPayments 
WHERE		(Product = 'S8' OR Product LIKE 'S8.%') 
		--	AND NextPaymentDate <> '0001-01-01' 
			AND NextPaymentDate >= (SELECT Convert(DATETIME, variable, 103) FROM tblVariables WHERE No = 1) 
			AND NextPaymentDate <= DATEADD(year, 1, (SELECT Convert(DATETIME, variable, 103) FROM tblVariables WHERE No = 1))
GO
/****** Object:  View [dbo].[S8PaymentSummary]    Script Date: 2/11/2023 1:33:40 pm ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE VIEW [dbo].[S8PaymentSummary] AS
SELECT		t1.AccessNo, t2.Product, t1.NextPaymentDate AS S8MaturityDate, t1.S8BalPerMaturity, t2.S8BalPerProduct, t3.S8BalPerClient
FROM		(SELECT AccessNo, NextPaymentDate, Product, SUM(Amount) AS S8BalPerMaturity FROM S8PaymentDetails GROUP BY AccessNo, Product, NextPaymentDate) t1
			LEFT JOIN (SELECT AccessNo, Product, SUM(Amount) AS S8BalPerProduct FROM S8PaymentDetails GROUP BY AccessNo, Product) t2 ON t1.AccessNo = t2.AccessNo AND t1.Product = t2.Product
			LEFT JOIN (SELECT AccessNo, SUM(Amount) AS S8BalPerClient FROM S8PaymentDetails GROUP BY AccessNo) t3 ON t1.AccessNo = t3.AccessNo
GO
/****** Object:  View [dbo].[ClientGrouping]    Script Date: 2/11/2023 1:33:40 pm ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE VIEW [dbo].[ClientGrouping]
AS

SELECT [GroupAccountNo], [GroupName], [AccountNo], [UltracsCounterparty], [ClientType], [RBNZResidencyMapping1]
FROM MRU_Retail_ClientGroup_Mapping

UNION 

SELECT [GroupAccountNo], [GroupName], [AccountNo], [UltracsCounterparty], [ClientType], [RBNZResidencyMapping1]
FROM MRU_TMS_ClientGroup_Mapping;

GO
/****** Object:  View [dbo].[TMSDeals]    Script Date: 2/11/2023 1:33:40 pm ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO



CREATE VIEW [dbo].[TMSDeals] AS

/********************************************************************************************************
 * This stores TMS deals sourced from an input table MRU_TMS_Liquidity_Reporting which is an csv output from TMS (Analytics).
 *
 ********************************************************************************************************
 *
 * @version 1.0
 * @author	Stephen Chin
 * @date	2020.08.13
 * 
 * - Start version control
 *
 * @version 1.1
 * @author	Stephen Chin
 * @date	2020.11.27
 * - Cast dealt rate to incorporate negative interest rate (sign)
 *
 * @version 1.2 - funding value: SQL model change 1.11 - Mapping face value to Funding value & adding Carrying Value and CCYCarrying Value
 * @author My Phan
 * @date 2022.05.17
 * - Mapping Face value
 *		-CAST(ISNULL(REPLACE(REPLACE(REPLACE([NZD Face Value], ')', ''), '(', '-') ,',',''), 0) AS DECIMAL(18,2)) AS Amount,
 *		-CAST(ISNULL(REPLACE(REPLACE(REPLACE([Face Value], ')', ''), '(', '-') ,',',''), 0) AS DECIMAL(18,2)) AS EOD_CCYBalance,
 * - Adding Carrying value fields
 *		-CAST(ISNULL(REPLACE(REPLACE(REPLACE([Carrying Value], ')', ''), '(', '-') ,',',''), 0) AS DECIMAL(18,2)) AS CCYCarryingValue, 
 *      -CAST(ISNULL(REPLACE(REPLACE(REPLACE([NZD Carrying Value], ')', ''), '(', '-') ,',',''), 0) AS DECIMAL(18,2)) AS CarryingValue
 *
 * 
*/

SELECT	CAST([Position Date] AS DATE) AS EodDate, 
        CASE WHEN CG.GroupAccountNo IS NULL THEN TCM.AccountNo ELSE CG.GroupAccountNo END AS GroupAccountNo, 
        TLR.[Deal No] AS DealNo, 
        TCM.AccountNo AS AccountNo, 
        TLR.Counterparty AS SourceCounterparty, 
        TIM.ProductGroup AS Product, 
        TIM.ProductGroup AS ProductGroup, 
        CASE WHEN TDO.Residency IS NULL THEN 
			CASE WHEN CG.RBNZResidencyMapping1 IS NULL THEN
				CASE WHEN TCM.RBNZResidencyMapping1 IS NULL OR TCM.RBNZResidencyMapping1 = '' THEN 
					'Domestic' 
				ELSE 
					TCM.RBNZResidencyMapping1
                END
			ELSE 
				CG.RBNZResidencyMapping1
			END
		ELSE
			TDO.Residency 
        END AS Residency,
		CAST([Dealt Date] AS DATE) AS EntryDate, 
		CAST([Begin Date] AS DATE) AS LodegementDate, 
		CAST([End Date] AS DATE) AS MaturityDate, 
		CASE WHEN CAST([End Date] AS DATE) IS NULL OR [End Date] = '1899-12-30' THEN 
			CAST(0 AS nvarchar(5)) 
		ELSE
			CASE WHEN CAST([Begin Date] AS DATE) IS NULL OR [Begin Date] = '1899-12-30' THEN 
				CAST(0 AS nvarchar(5)) 
			ELSE 
				CAST(DATEDIFF(DD, CAST([Begin Date] AS DATE), CAST([End Date] AS DATE)) AS nvarchar(5))
			END
		END AS Term, 
		-- v1.2 - update amount and EOD_CCYBlance as Face Value
		-CAST(ISNULL(REPLACE(REPLACE(REPLACE([NZD Face Value], ')', ''), '(', '-') ,',',''), 0) AS DECIMAL(18,2)) AS Amount,
		-CAST(ISNULL(REPLACE(REPLACE(REPLACE([Face Value], ')', ''), '(', '-') ,',',''), 0) AS DECIMAL(18,2)) AS EOD_CCYBalance, 
        -CAST(ISNULL(REPLACE(REPLACE(REPLACE([NZD Face Value], ')', ''), '(', '-') ,',',''), 0) AS DECIMAL(18,2)) AS EOD_Balance, 
		-CAST(ISNULL(REPLACE(REPLACE(REPLACE([Face Value], ')', ''), '(', '-') ,',',''), 0) AS DECIMAL(18,2)) AS CCYFaceValue, 
        -CAST(ISNULL(REPLACE(REPLACE(REPLACE([NZD Face Value], ')', ''), '(', '-') ,',',''), 0) AS DECIMAL(18,2)) AS FaceValue,
        -CAST(ISNULL(REPLACE(REPLACE(REPLACE([Book Value], ')', ''), '(', '-') ,',',''), 0) AS DECIMAL(18,2)) AS CCYBookValue,
		-CAST(ISNULL(REPLACE(REPLACE(REPLACE([NZD Book Value], ')', ''), '(', '-') ,',',''), 0) AS DECIMAL(18,2)) AS BookValue,
        -CAST(ISNULL(REPLACE(REPLACE(REPLACE([Market Value], ')', ''), '(', '-') ,',',''), 0) AS DECIMAL(18,2)) AS CCYMarketValue, 
        -CAST(ISNULL(REPLACE(REPLACE(REPLACE([NZD Market Value], ')', ''), '(', '-') ,',',''), 0) AS DECIMAL(18,2)) AS MarketValue,
        TLR.Currency AS CCY, 
        CAST(TLR.[Spot Factor] AS float) AS FXSpotFactor, 
        0 AS SumOfEODLimit, 
        'TMS' AS [Source],
		CAST(REPLACE(REPLACE(TLR.[Dealt Rate], ')', ''), '(', '-') AS float) AS DealtRate,
		TLR.Instrument AS Instrument,
		-- v1.2 adding carrying value
		-CAST(ISNULL(REPLACE(REPLACE(REPLACE([Carrying Value], ')', ''), '(', '-') ,',',''), 0) AS DECIMAL(18,2)) AS CCYCarryingValue, 
        -CAST(ISNULL(REPLACE(REPLACE(REPLACE([NZD Carrying Value], ')', ''), '(', '-') ,',',''), 0) AS DECIMAL(18,2)) AS CarryingValue

FROM	MRU_TMS_Liquidity_Reporting TLR
        LEFT JOIN MRU_TMS_Client_Mapping TCM ON TLR.Counterparty = TCM.TMSCounterparty
        LEFT JOIN MRU_TMS_Instrument_Mapping TIM ON TLR.Instrument = TIM.SourceInstrumentName
        LEFT JOIN ClientGrouping CG ON TCM.AccountNo = CG.AccountNo
        LEFT JOIN MRU_TMS_Deal_Override TDO ON TLR.[Deal Id] = TDO.[Deal Id]  AND TLR.[Deal No] = TDO.[Deal No] AND TLR.[Deal Side] = TDO.[Deal Side]

WHERE  ((TIM.ProductGroup LIKE 'RCD%' OR TIM.ProductGroup LIKE 'SEN%' OR TIM.ProductGroup LIKE 'SUB%' OR TIM.ProductGroup LIKE 'MTN%' OR TIM.ProductGroup LIKE 'REP%' OR TIM.ProductGroup LIKE 'ECP%' OR TIM.ProductGroup LIKE 'COV%') AND TLR.[Settle Status] = 'Settled') 
        OR (TIM.ProductGroup LIKE 'FXFUND%' AND TLR.[Settle Status] = 'Settled' AND TLR.[Adj Mapping Profile] = 'MMFund') 
        OR (TIM.ProductGroup LIKE 'MMCSH%' AND TLR.[Settle Status] = 'Settled' AND TLR.[Adj Mapping Profile] = 'MMFund') 
        OR (TLR.[Settle Status] = 'Settled' AND (TLR.Instrument = 'External Term Deposit' OR TLR.Instrument = 'External Call Deposit' OR TLR.Instrument = 'External TD Cmpnd Qtly' OR TLR.Instrument = 'External TD Cmpnd Semi An' OR TLR.Instrument = 'Extendible Term Deposit'))
        OR (TIM.ProductGroup = 'FCA' AND TLR.[Settle Status] = 'Settled');

		
GO
/****** Object:  View [dbo].[Variables]    Script Date: 2/11/2023 1:33:40 pm ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO



CREATE VIEW [dbo].[Variables]
AS
SELECT *
FROM (SELECT CONVERT(DATETIME, Variable, 103) as Setting, CASE [No] WHEN 1 THEN 'ReportEndDate' WHEN 2 THEN 'ReportStartDate' WHEN 3 THEN 'TMSProcessUnlockDate' WHEN 4 THEN 'RetailProcessUnlockDate' END AS Name
FROM tblVariables) X
PIVOT (Max(Setting) FOR [Name] IN ([ReportEndDate],[ReportStartDate],[TMSProcessUnlockDate],[RetailProcessUnlockDate])) AS Value


GO
/****** Object:  View [dbo].[AllData]    Script Date: 2/11/2023 1:33:40 pm ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


CREATE VIEW [dbo].[AllData] AS

/********************************************************************************************************
 * This view stores the most comprehensive funding details and mainly used for master soruce of output tables.
 * It appends additional fields, original term days, remaining term days, funding type and category to MRU_Liquidity_Funding_Balances_Details_Bandings.
 *
 ********************************************************************************************************
 *
 * >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
 * @version 1.0
 * @author	Stephen Chin
 * @date	2020.08.13
 * 
 * - Start version control
 * - Add new time bucket code '50Y' to existing time profile sets
 * 
 * >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
 * @version 1.1
 * @author	Stephen Chin
 * @date	2021.04.12
 *
 * - Apply BS13 paragraph 39 Core Funding Ratio's treatment of FLP funding
 * - "50 per cent of any tradable debt securities issued by the bank or funding from Reserve Bank facilities,
 *    with original maturity of two years or more and with residential maturity at the reporting date of
 *    more than six months and not more than one year"	
 *
*/


SELECT	*, 
		CASE WHEN dbo.GetTimeProfile(EodDate, RemaingTermDays) IN ('18M', '2Y', '3Y', '4Y', '5Y', '7Y', '10Y', '15Y', '20Y', '50Y') THEN 
			'All funding greater than 1 year'
		ELSE
			CASE WHEN TradeableFlag = 'Y' THEN
				CASE WHEN ClientType = 'NP' THEN
					'Non Market funding less than or equal to 1 year'
				ELSE
					CASE WHEN OriginalTermDays = DATEDIFF(DD, LodegementDate, DATEADD(YY, 2, LodegementDate)) OR dbo.GetTimeProfile(ISNULL(LodegementDate, EodDate), OriginalTermDays) IN ('3Y', '4Y', '5Y', '7Y', '10Y', '15Y', '20Y', '50Y') THEN 
						CASE WHEN dbo.GetTimeProfile(EodDate, RemaingTermDays) IN ('1Y') THEN
							'Tradeable securities at 50 percent'
						ELSE
							'Market funding less than or equal to 1 year'
						END
					ELSE
						'Market funding less than or equal to 1 year'
					END
				END
			ELSE
				CASE WHEN ClientType = 'FI' THEN
					CASE WHEN Identification IN (Select DealNo From MRU_TMS_Deal_Override_FLP)
							  AND (OriginalTermDays = DATEDIFF(DD, LodegementDate, DATEADD(YY, 2, LodegementDate)) OR dbo.GetTimeProfile(ISNULL(LodegementDate, EodDate), OriginalTermDays) IN ('3Y', '4Y', '5Y', '7Y', '10Y', '15Y', '20Y', '50Y')) 
							  AND dbo.GetTimeProfile(EodDate, RemaingTermDays) IN ('1Y') THEN
						'Tradeable securities at 50 percent'
					ELSE
						'Market funding less than or equal to 1 year'
					END
				ELSE
					CASE WHEN ClientType LIKE 'INTERNAL%' THEN
						'Market funding less than or equal to 1 year'
					ELSE
						'Non Market funding less than or equal to 1 year'
					END
				END
			END
		END AS Category

FROM	(
		SELECT	FBDB.*, 
				
				CASE WHEN ProductGroup NOT LIKE 'S%' AND ProductGroup NOT LIKE 'FCA%' THEN
					DATEDIFF(D, LodegementDate, MaturityDate)
				ELSE
					CASE WHEN ProductGroup LIKE 'SE%' THEN
						DATEDIFF(D, LodegementDate, MaturityDate)
					ELSE
						CASE WHEN ProductGroup LIKE 'SU%' THEN
							DATEDIFF(D, LodegementDate, MaturityDate)
						ELSE
							CASE WHEN ProductGroup = 'S8' THEN
								32
							ELSE
								0
							END
						END
					END
				END AS OriginalTermDays,
				 
				CASE WHEN ProductGroup NOT LIKE 'S%' THEN
					CASE WHEN MaturityDate IS NULL THEN
						0
					ELSE
						CASE WHEN DATEDIFF(D, Variables.ReportEndDate, MaturityDate) < 0 THEN
							0
						ELSE
							DATEDIFF(D, Variables.ReportEndDate, MaturityDate)
						END
					END
				ELSE
					CASE WHEN ProductGroup LIKE 'SE%' THEN 
						DATEDIFF(D, Variables.ReportEndDate, MaturityDate)
					ELSE
						CASE WHEN ProductGroup LIKE 'SU%' THEN
							DATEDIFF(D, Variables.ReportEndDate, MaturityDate)
						ELSE
							CASE WHEN ProductGroup = 'S8' THEN
								CASE WHEN (MaturityDate IS NULL OR MaturityDate > DATEADD(DD, 90, (Select Convert(DATETIME, variable, 103) From tblVariables Where No = 1))) THEN 32 ELSE DATEDIFF(D, Variables.ReportEndDate, MaturityDate) END
							ELSE
								0
							END
						END
					END
				END AS RemaingTermDays, 

				CASE WHEN TradeableFlag = 'Y' THEN
					CASE WHEN ClientType = 'NP' THEN
						'Non Market Funding'
					ELSE
						'Market Funding'
					END
				ELSE
					CASE WHEN ClientType = 'FI' THEN
						'Market Funding'
					ELSE
						CASE WHEN ClientType LIKE 'INTERNAL%' THEN
							'Market Funding'
						ELSE
							'Non Market Funding'
						END
					END
				END AS FundingType
				
				FROM	MRU_Liquidity_Funding_Balances_Details_Bandings FBDB, 
						Variables
		) Detail;

GO
/****** Object:  View [dbo].[CustomerStatic]    Script Date: 2/11/2023 1:33:40 pm ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE VIEW [dbo].[CustomerStatic]
AS
SELECT	AccessNo, Surname, Forenames, Title, ResAddr1, ResAddr2, ResAddr3, ResCountry, WHtaxExempt, NonResCode, ClientType
FROM	Ultracs.CustomersKB CS
		LEFT JOIN MRU_Retail_ClientType_Mapping TM ON CS.AccessNo = TM.AccountNo

UNION ALL

SELECT	AccessNo, Surname, Forenames, Title, ResAddr1, ResAddr2, ResAddr3, ResCountry, WHtaxExempt, NonResCode, ClientType
FROM	Ultracs.CustomersNZHL CS
		LEFT JOIN MRU_Retail_ClientType_Mapping TM ON CS.AccessNo = TM.AccountNo
		
UNION ALL

SELECT	AccessNo, Surname, Forenames, Title, ResAddr1, ResAddr2, ResAddr3, ResCountry, WHtaxExempt, NonResCode, ClientType
FROM	Ultracs.CustomersAMP CS
		LEFT JOIN MRU_Retail_ClientType_Mapping TM ON CS.AccessNo = TM.AccountNo;

GO
/****** Object:  View [dbo].[ClientBalance]    Script Date: 2/11/2023 1:33:40 pm ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE VIEW [dbo].[ClientBalance]
AS
/********************************************************************************************************
 * This binds retail and TMS clients' balance.
 *
 ********************************************************************************************************
 *
 * @version 1.0
 * @author	Stephen Chin
 * @date	2020.08.13
 * 
 * - Start version control
 *
*/

SELECT	FB.EodDate AS EodDate,
	   	FB.GroupAccountNo AS GroupAccountNo, 
	   	FB.AccountNo AS AccountNo,
		CASE WHEN CS.Forenames IS NULL THEN CS.Surname ELSE LEFT(CS.Forenames,1) + ' ' + CS.Surname END AS AccountName, 
	   	FB.Product AS Product, 
	   	FB.EOD_Balance AS EOD_Balance, 
	   	FB.SumOfEODLimit AS SumOfEODLimit
FROM 	MRU_Liquidity_Funding_Balances FB 
		LEFT JOIN CustomerStatic CS ON FB.AccountNo = CS.AccessNo

UNION ALL 

SELECT	TMS.EodDate AS EodDate,  
	   	TMS.GroupAccountNo AS GroupAccountNo, 
	   	TMS.AccountNo AS AccountNo,
		TMS.SourceCounterparty AS AccountName,
	   	TMS.Product AS Product,  
	   	TMS.EOD_Balance AS EOD_Balance,  
	   	TMS.SumOfEODLimit AS SumOfEODLimit
FROM 	TMSDeals TMS;

GO
/****** Object:  View [dbo].[TMSCashflows]    Script Date: 2/11/2023 1:33:40 pm ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE VIEW [dbo].[TMSCashflows]
AS

/********************************************************************************************************
 * This is a miscellaneous view that stores TMS cashflow detail and not used in regulatory calculation.
 *
 ********************************************************************************************************
 *
 * @version 1.0
 * @author	Stephen Chin
 * @date	2020.08.13
 * 
 * - Start version control
 *
 *
*/

SELECT	TMS.[D1Deal No], 
		TMS.Instrument, 
		TMS.Entity, 
		TMS.[Settle Status], 
		TMS.[Flow Type], 
		TMS.[Type], 
		TMS.RepoFlag, 
		TMS.Sector, 
		TMS.Currency, 
		TMS.[Cash Flow Date] AS CashFlowDate, 
		ISNULL(DATEDIFF(d,ReportEndDate,[Cash Flow Date]), 0) AS DaysToMaturity, 
		TMS.[Adj Time Profile], 
		CFM.IncludePrincipal,
		CAST(REPLACE([NZD Principal Cashflow],',','') AS FLOAT) AS NZDPrincipalCashflow, 
		CFM.IncludeInterest, 
		CAST(REPLACE([NZD Interest Cashflow],',','') AS FLOAT) AS NZDInterestCashflow, 
		CFM.IncludeOther, 
		CAST(REPLACE([NZD Other Cashflow],',','') AS FLOAT) AS NZDOtherCashflow, 
		CASE WHEN CFM.[IncludePrincipal] <> 'N' THEN CAST(REPLACE([NZD Principal Cashflow],',','') AS FLOAT) ELSE 0 END + CASE WHEN CFM.[IncludeInterest] <> 'N' THEN CAST(REPLACE([NZD Interest Cashflow],',','') AS FLOAT) ELSE 0 END + CASE WHEN CFM.[IncludeOther] <> 'N' THEN CAST(REPLACE([NZD Other Cashflow],',','') AS FLOAT) ELSE 0 END AS NZDTotalCashflow, 
		dbo.GetBucketProfileFunding(dbo.GetTimeProfile(Variables.ReportEndDate, ISNULL(DATEDIFF(d,ReportEndDate,[Cash Flow Date]), 0))) AS BucketProfileCashflow

FROM	MRU_TMS_Liquidity_Cashflows TMS
		LEFT JOIN MRU_Cashflow_Mapping CFM ON TMS.Sector = CFM.Sector AND TMS.[Type] = CFM.[Type]
		LEFT JOIN MRU_TimeProfile_Sorting TFS ON TMS.[Adj Time Profile] = TFS.TimeProfile,
		Variables

WHERE	CAST(REPLACE([NZD Principal Cashflow] ,',','') AS DECIMAL(18,2)) <> 0 
		OR CAST(REPLACE([NZD Interest Cashflow],',','') AS DECIMAL(18,2)) <> 0 
		OR CAST(REPLACE([NZD Other Cashflow],',','') AS DECIMAL(18,2)) <> 0;

GO
/****** Object:  View [dbo].[ProductBalance]    Script Date: 2/11/2023 1:33:40 pm ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE VIEW [dbo].[ProductBalance]
AS

/********************************************************************************************************
 * Compares product balances between liquidity and sources (either retail or TMS).
 * This is more granular view than BalanceAdjustment, and expected to have zero difference except 
 * - FCA and 
 * - Credit cards (S71/72/78/83/87)
 * - AccountNo = '2345525' in 'S30%' (excluded explicitly due to Kiwibank internal account)
 *
 * Otherwise, further investigation is required.
 * Note this view is used for operational control purpose and does not interact with daily regulatory calculation.
 *
 ********************************************************************************************************
 *
 * @version 1.0
 * @author	Stephen Chin
 * @date	2020.09.03
 * 
 * - Start version control
 *
 *
*/

SELECT	LIQ.EodDate, LIQ.ProductGroup, LIQ.Balance AS LiqFundingBalance, SRC.Balance AS SrcFundingBalance, ISNULL(LIQ.balance, 0) - ISNULL(SRC.balance, 0) AS Diff, SRC.Source
FROM	(Select	EodDate, ProductGroup, SUM(Balance) AS Balance From	AllData Group By EodDate, ProductGroup) LIQ
		LEFT JOIN 
		(
		-- (a) Retail/Business funding balance
		SELECT		dbo.GetProductGroup(Product) AS ProductGroup, 
					CASE WHEN dbo.GetProductGroup(Product) = 'S76' THEN NULL ELSE SUM(EodBalance) END AS Balance,
					'Retail' AS Source
		FROM		Ultracs.AccountsEOD
		WHERE		EodBalance > 0
					AND dbo.GetProductGroup(Product) IN (Select ProductGrp From MRU_Product_Mapping Where Sector <> 'Wholesale')
		GROUP BY	dbo.GetProductGroup(Product)

		UNION ALL

		-- (b) Wholesale funding balance
		SELECT		ProductGroup,
					CASE WHEN TIM.ProductGroup = 'MMCSH' AND RPT.[Adj Mapping Profile] = 'MMFund' THEN 
						ABS(SUM(CAST(ISNULL(REPLACE(REPLACE(REPLACE([NZD Carrying Value], ')', ''), '(', '-') ,',',''), 0) AS money))) 
					ELSE
						CASE WHEN TIM.ProductGroup <> 'MMCSH' THEN
							ABS(SUM(CAST(ISNULL(REPLACE(REPLACE(REPLACE([NZD Carrying Value], ')', ''), '(', '-') ,',',''), 0) AS money)))
						END
					END AS Balance,
					'TMS' AS Source
		FROM		MRU_TMS_Liquidity_Reporting RPT
					LEFT JOIN MRU_TMS_Instrument_Mapping TIM ON RPT.Instrument = TIM.SourceInstrumentName
		WHERE		ProductGroup IN (Select ProductGrp From MRU_Product_Mapping Where Sector IN ('Wholesale')) 
					AND [Adj Mapping Profile] NOT IN ('TreasuryAssets')
					AND [Settle Status] = 'Settled'
		GROUP BY	ProductGroup, [Adj Mapping Profile]
		) SRC ON LIQ.ProductGroup = SRC.ProductGroup;
		
GO
/****** Object:  UserDefinedFunction [cdc].[fn_cdc_get_all_changes_dbo_MRU_Cashflow_Mapping]    Script Date: 2/11/2023 1:33:40 pm ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

	create function [cdc].[fn_cdc_get_all_changes_dbo_MRU_Cashflow_Mapping]
	(	@from_lsn binary(10),
		@to_lsn binary(10),
		@row_filter_option nvarchar(30)
	)
	returns table
	return
	
	select NULL as __$start_lsn,
		NULL as __$seqval,
		NULL as __$operation,
		NULL as __$update_mask, NULL as [Type], NULL as [Sector], NULL as [IncludePrincipal], NULL as [IncludeInterest], NULL as [IncludeOther]
	where ( [sys].[fn_cdc_check_parameters]( N'dbo_MRU_Cashflow_Mapping', @from_lsn, @to_lsn, lower(rtrim(ltrim(@row_filter_option))), 0) = 0)

	union all
	
	select t.__$start_lsn as __$start_lsn,
		t.__$seqval as __$seqval,
		t.__$operation as __$operation,
		t.__$update_mask as __$update_mask, t.[Type], t.[Sector], t.[IncludePrincipal], t.[IncludeInterest], t.[IncludeOther]
	from [cdc].[dbo_MRU_Cashflow_Mapping_CT] t with (nolock)    
	where (lower(rtrim(ltrim(@row_filter_option))) = 'all')
	    and ( [sys].[fn_cdc_check_parameters]( N'dbo_MRU_Cashflow_Mapping', @from_lsn, @to_lsn, lower(rtrim(ltrim(@row_filter_option))), 0) = 1)
		and (t.__$operation = 1 or t.__$operation = 2 or t.__$operation = 4)
		and (t.__$start_lsn <= @to_lsn)
		and (t.__$start_lsn >= @from_lsn)
		
	union all	
		
	select t.__$start_lsn as __$start_lsn,
		t.__$seqval as __$seqval,
		t.__$operation as __$operation,
		t.__$update_mask as __$update_mask, t.[Type], t.[Sector], t.[IncludePrincipal], t.[IncludeInterest], t.[IncludeOther]
	from [cdc].[dbo_MRU_Cashflow_Mapping_CT] t with (nolock)     
	where (lower(rtrim(ltrim(@row_filter_option))) = 'all update old')
	    and ( [sys].[fn_cdc_check_parameters]( N'dbo_MRU_Cashflow_Mapping', @from_lsn, @to_lsn, lower(rtrim(ltrim(@row_filter_option))), 0) = 1)
		and (t.__$operation = 1 or t.__$operation = 2 or t.__$operation = 4 or
		     t.__$operation = 3 )
		and (t.__$start_lsn <= @to_lsn)
		and (t.__$start_lsn >= @from_lsn)
	
GO
/****** Object:  UserDefinedFunction [cdc].[fn_cdc_get_all_changes_dbo_MRU_Product_Mapping]    Script Date: 2/11/2023 1:33:40 pm ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

	create function [cdc].[fn_cdc_get_all_changes_dbo_MRU_Product_Mapping]
	(	@from_lsn binary(10),
		@to_lsn binary(10),
		@row_filter_option nvarchar(30)
	)
	returns table
	return
	
	select NULL as __$start_lsn,
		NULL as __$seqval,
		NULL as __$operation,
		NULL as __$update_mask, NULL as [ProductGrp], NULL as [ProductName], NULL as [ProductType], NULL as [TradeableFlag], NULL as [NetGross], NULL as [Sector], NULL as [NormalState], NULL as [RBNZProductMapping1], NULL as [RBNZProductMapping2]
	where ( [sys].[fn_cdc_check_parameters]( N'dbo_MRU_Product_Mapping', @from_lsn, @to_lsn, lower(rtrim(ltrim(@row_filter_option))), 0) = 0)

	union all
	
	select t.__$start_lsn as __$start_lsn,
		t.__$seqval as __$seqval,
		t.__$operation as __$operation,
		t.__$update_mask as __$update_mask, t.[ProductGrp], t.[ProductName], t.[ProductType], t.[TradeableFlag], t.[NetGross], t.[Sector], t.[NormalState], t.[RBNZProductMapping1], t.[RBNZProductMapping2]
	from [cdc].[dbo_MRU_Product_Mapping_CT] t with (nolock)    
	where (lower(rtrim(ltrim(@row_filter_option))) = 'all')
	    and ( [sys].[fn_cdc_check_parameters]( N'dbo_MRU_Product_Mapping', @from_lsn, @to_lsn, lower(rtrim(ltrim(@row_filter_option))), 0) = 1)
		and (t.__$operation = 1 or t.__$operation = 2 or t.__$operation = 4)
		and (t.__$start_lsn <= @to_lsn)
		and (t.__$start_lsn >= @from_lsn)
		
	union all	
		
	select t.__$start_lsn as __$start_lsn,
		t.__$seqval as __$seqval,
		t.__$operation as __$operation,
		t.__$update_mask as __$update_mask, t.[ProductGrp], t.[ProductName], t.[ProductType], t.[TradeableFlag], t.[NetGross], t.[Sector], t.[NormalState], t.[RBNZProductMapping1], t.[RBNZProductMapping2]
	from [cdc].[dbo_MRU_Product_Mapping_CT] t with (nolock)     
	where (lower(rtrim(ltrim(@row_filter_option))) = 'all update old')
	    and ( [sys].[fn_cdc_check_parameters]( N'dbo_MRU_Product_Mapping', @from_lsn, @to_lsn, lower(rtrim(ltrim(@row_filter_option))), 0) = 1)
		and (t.__$operation = 1 or t.__$operation = 2 or t.__$operation = 4 or
		     t.__$operation = 3 )
		and (t.__$start_lsn <= @to_lsn)
		and (t.__$start_lsn >= @from_lsn)
	
GO
/****** Object:  UserDefinedFunction [cdc].[fn_cdc_get_all_changes_dbo_MRU_RBNZ_CoverFactors]    Script Date: 2/11/2023 1:33:40 pm ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

	create function [cdc].[fn_cdc_get_all_changes_dbo_MRU_RBNZ_CoverFactors]
	(	@from_lsn binary(10),
		@to_lsn binary(10),
		@row_filter_option nvarchar(30)
	)
	returns table
	return
	
	select NULL as __$start_lsn,
		NULL as __$seqval,
		NULL as __$operation,
		NULL as __$update_mask, NULL as [Class], NULL as [Rating], NULL as [Tier1Haircut], NULL as [Tier2Haircut]
	where ( [sys].[fn_cdc_check_parameters]( N'dbo_MRU_RBNZ_CoverFactors', @from_lsn, @to_lsn, lower(rtrim(ltrim(@row_filter_option))), 0) = 0)

	union all
	
	select t.__$start_lsn as __$start_lsn,
		t.__$seqval as __$seqval,
		t.__$operation as __$operation,
		t.__$update_mask as __$update_mask, t.[Class], t.[Rating], t.[Tier1Haircut], t.[Tier2Haircut]
	from [cdc].[dbo_MRU_RBNZ_CoverFactors_CT] t with (nolock)    
	where (lower(rtrim(ltrim(@row_filter_option))) = 'all')
	    and ( [sys].[fn_cdc_check_parameters]( N'dbo_MRU_RBNZ_CoverFactors', @from_lsn, @to_lsn, lower(rtrim(ltrim(@row_filter_option))), 0) = 1)
		and (t.__$operation = 1 or t.__$operation = 2 or t.__$operation = 4)
		and (t.__$start_lsn <= @to_lsn)
		and (t.__$start_lsn >= @from_lsn)
		
	union all	
		
	select t.__$start_lsn as __$start_lsn,
		t.__$seqval as __$seqval,
		t.__$operation as __$operation,
		t.__$update_mask as __$update_mask, t.[Class], t.[Rating], t.[Tier1Haircut], t.[Tier2Haircut]
	from [cdc].[dbo_MRU_RBNZ_CoverFactors_CT] t with (nolock)     
	where (lower(rtrim(ltrim(@row_filter_option))) = 'all update old')
	    and ( [sys].[fn_cdc_check_parameters]( N'dbo_MRU_RBNZ_CoverFactors', @from_lsn, @to_lsn, lower(rtrim(ltrim(@row_filter_option))), 0) = 1)
		and (t.__$operation = 1 or t.__$operation = 2 or t.__$operation = 4 or
		     t.__$operation = 3 )
		and (t.__$start_lsn <= @to_lsn)
		and (t.__$start_lsn >= @from_lsn)
	
GO
/****** Object:  UserDefinedFunction [cdc].[fn_cdc_get_all_changes_dbo_MRU_RBNZ_New_Programme_Issues]    Script Date: 2/11/2023 1:33:40 pm ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

	create function [cdc].[fn_cdc_get_all_changes_dbo_MRU_RBNZ_New_Programme_Issues]
	(	@from_lsn binary(10),
		@to_lsn binary(10),
		@row_filter_option nvarchar(30)
	)
	returns table
	return
	
	select NULL as __$start_lsn,
		NULL as __$seqval,
		NULL as __$operation,
		NULL as __$update_mask, NULL as [Id], NULL as [Source], NULL as [Identification], NULL as [Product], NULL as [Account No], NULL as [CCY], NULL as [Face Value], NULL as [FXRateTo_NZD], NULL as [Lodgement Date], NULL as [Maturity Date], NULL as [Interpolated_BKBM], NULL as [Margin_BKBM], NULL as [Notes]
	where ( [sys].[fn_cdc_check_parameters]( N'dbo_MRU_RBNZ_New_Programme_Issues', @from_lsn, @to_lsn, lower(rtrim(ltrim(@row_filter_option))), 0) = 0)

	union all
	
	select t.__$start_lsn as __$start_lsn,
		t.__$seqval as __$seqval,
		t.__$operation as __$operation,
		t.__$update_mask as __$update_mask, t.[Id], t.[Source], t.[Identification], t.[Product], t.[Account No], t.[CCY], t.[Face Value], t.[FXRateTo_NZD], t.[Lodgement Date], t.[Maturity Date], t.[Interpolated_BKBM], t.[Margin_BKBM], t.[Notes]
	from [cdc].[dbo_MRU_RBNZ_New_Programme_Issues_CT] t with (nolock)    
	where (lower(rtrim(ltrim(@row_filter_option))) = 'all')
	    and ( [sys].[fn_cdc_check_parameters]( N'dbo_MRU_RBNZ_New_Programme_Issues', @from_lsn, @to_lsn, lower(rtrim(ltrim(@row_filter_option))), 0) = 1)
		and (t.__$operation = 1 or t.__$operation = 2 or t.__$operation = 4)
		and (t.__$start_lsn <= @to_lsn)
		and (t.__$start_lsn >= @from_lsn)
		
	union all	
		
	select t.__$start_lsn as __$start_lsn,
		t.__$seqval as __$seqval,
		t.__$operation as __$operation,
		t.__$update_mask as __$update_mask, t.[Id], t.[Source], t.[Identification], t.[Product], t.[Account No], t.[CCY], t.[Face Value], t.[FXRateTo_NZD], t.[Lodgement Date], t.[Maturity Date], t.[Interpolated_BKBM], t.[Margin_BKBM], t.[Notes]
	from [cdc].[dbo_MRU_RBNZ_New_Programme_Issues_CT] t with (nolock)     
	where (lower(rtrim(ltrim(@row_filter_option))) = 'all update old')
	    and ( [sys].[fn_cdc_check_parameters]( N'dbo_MRU_RBNZ_New_Programme_Issues', @from_lsn, @to_lsn, lower(rtrim(ltrim(@row_filter_option))), 0) = 1)
		and (t.__$operation = 1 or t.__$operation = 2 or t.__$operation = 4 or
		     t.__$operation = 3 )
		and (t.__$start_lsn <= @to_lsn)
		and (t.__$start_lsn >= @from_lsn)
	
GO
/****** Object:  UserDefinedFunction [cdc].[fn_cdc_get_all_changes_dbo_MRU_Retail_ClientType_Mapping]    Script Date: 2/11/2023 1:33:40 pm ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

	create function [cdc].[fn_cdc_get_all_changes_dbo_MRU_Retail_ClientType_Mapping]
	(	@from_lsn binary(10),
		@to_lsn binary(10),
		@row_filter_option nvarchar(30)
	)
	returns table
	return
	
	select NULL as __$start_lsn,
		NULL as __$seqval,
		NULL as __$operation,
		NULL as __$update_mask, NULL as [AccountNo], NULL as [ClientType], NULL as [RBNZResidencyMapping1]
	where ( [sys].[fn_cdc_check_parameters]( N'dbo_MRU_Retail_ClientType_Mapping', @from_lsn, @to_lsn, lower(rtrim(ltrim(@row_filter_option))), 0) = 0)

	union all
	
	select t.__$start_lsn as __$start_lsn,
		t.__$seqval as __$seqval,
		t.__$operation as __$operation,
		t.__$update_mask as __$update_mask, t.[AccountNo], t.[ClientType], t.[RBNZResidencyMapping1]
	from [cdc].[dbo_MRU_Retail_ClientType_Mapping_CT] t with (nolock)    
	where (lower(rtrim(ltrim(@row_filter_option))) = 'all')
	    and ( [sys].[fn_cdc_check_parameters]( N'dbo_MRU_Retail_ClientType_Mapping', @from_lsn, @to_lsn, lower(rtrim(ltrim(@row_filter_option))), 0) = 1)
		and (t.__$operation = 1 or t.__$operation = 2 or t.__$operation = 4)
		and (t.__$start_lsn <= @to_lsn)
		and (t.__$start_lsn >= @from_lsn)
		
	union all	
		
	select t.__$start_lsn as __$start_lsn,
		t.__$seqval as __$seqval,
		t.__$operation as __$operation,
		t.__$update_mask as __$update_mask, t.[AccountNo], t.[ClientType], t.[RBNZResidencyMapping1]
	from [cdc].[dbo_MRU_Retail_ClientType_Mapping_CT] t with (nolock)     
	where (lower(rtrim(ltrim(@row_filter_option))) = 'all update old')
	    and ( [sys].[fn_cdc_check_parameters]( N'dbo_MRU_Retail_ClientType_Mapping', @from_lsn, @to_lsn, lower(rtrim(ltrim(@row_filter_option))), 0) = 1)
		and (t.__$operation = 1 or t.__$operation = 2 or t.__$operation = 4 or
		     t.__$operation = 3 )
		and (t.__$start_lsn <= @to_lsn)
		and (t.__$start_lsn >= @from_lsn)
	
GO
/****** Object:  UserDefinedFunction [cdc].[fn_cdc_get_all_changes_dbo_MRU_TimeProfile_Sorting]    Script Date: 2/11/2023 1:33:40 pm ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

	create function [cdc].[fn_cdc_get_all_changes_dbo_MRU_TimeProfile_Sorting]
	(	@from_lsn binary(10),
		@to_lsn binary(10),
		@row_filter_option nvarchar(30)
	)
	returns table
	return
	
	select NULL as __$start_lsn,
		NULL as __$seqval,
		NULL as __$operation,
		NULL as __$update_mask, NULL as [TimeProfile], NULL as [TimeProfileSort]
	where ( [sys].[fn_cdc_check_parameters]( N'dbo_MRU_TimeProfile_Sorting', @from_lsn, @to_lsn, lower(rtrim(ltrim(@row_filter_option))), 0) = 0)

	union all
	
	select t.__$start_lsn as __$start_lsn,
		t.__$seqval as __$seqval,
		t.__$operation as __$operation,
		t.__$update_mask as __$update_mask, t.[TimeProfile], t.[TimeProfileSort]
	from [cdc].[dbo_MRU_TimeProfile_Sorting_CT] t with (nolock)    
	where (lower(rtrim(ltrim(@row_filter_option))) = 'all')
	    and ( [sys].[fn_cdc_check_parameters]( N'dbo_MRU_TimeProfile_Sorting', @from_lsn, @to_lsn, lower(rtrim(ltrim(@row_filter_option))), 0) = 1)
		and (t.__$operation = 1 or t.__$operation = 2 or t.__$operation = 4)
		and (t.__$start_lsn <= @to_lsn)
		and (t.__$start_lsn >= @from_lsn)
		
	union all	
		
	select t.__$start_lsn as __$start_lsn,
		t.__$seqval as __$seqval,
		t.__$operation as __$operation,
		t.__$update_mask as __$update_mask, t.[TimeProfile], t.[TimeProfileSort]
	from [cdc].[dbo_MRU_TimeProfile_Sorting_CT] t with (nolock)     
	where (lower(rtrim(ltrim(@row_filter_option))) = 'all update old')
	    and ( [sys].[fn_cdc_check_parameters]( N'dbo_MRU_TimeProfile_Sorting', @from_lsn, @to_lsn, lower(rtrim(ltrim(@row_filter_option))), 0) = 1)
		and (t.__$operation = 1 or t.__$operation = 2 or t.__$operation = 4 or
		     t.__$operation = 3 )
		and (t.__$start_lsn <= @to_lsn)
		and (t.__$start_lsn >= @from_lsn)
	
GO
/****** Object:  UserDefinedFunction [cdc].[fn_cdc_get_all_changes_dbo_MRU_TMS_Client_Mapping]    Script Date: 2/11/2023 1:33:40 pm ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

	create function [cdc].[fn_cdc_get_all_changes_dbo_MRU_TMS_Client_Mapping]
	(	@from_lsn binary(10),
		@to_lsn binary(10),
		@row_filter_option nvarchar(30)
	)
	returns table
	return
	
	select NULL as __$start_lsn,
		NULL as __$seqval,
		NULL as __$operation,
		NULL as __$update_mask, NULL as [AccountNo], NULL as [TMSCounterparty], NULL as [UltracsCounterparty], NULL as [ClientType], NULL as [RBNZResidencyMapping1]
	where ( [sys].[fn_cdc_check_parameters]( N'dbo_MRU_TMS_Client_Mapping', @from_lsn, @to_lsn, lower(rtrim(ltrim(@row_filter_option))), 0) = 0)

	union all
	
	select t.__$start_lsn as __$start_lsn,
		t.__$seqval as __$seqval,
		t.__$operation as __$operation,
		t.__$update_mask as __$update_mask, t.[AccountNo], t.[TMSCounterparty], t.[UltracsCounterparty], t.[ClientType], t.[RBNZResidencyMapping1]
	from [cdc].[dbo_MRU_TMS_Client_Mapping_CT] t with (nolock)    
	where (lower(rtrim(ltrim(@row_filter_option))) = 'all')
	    and ( [sys].[fn_cdc_check_parameters]( N'dbo_MRU_TMS_Client_Mapping', @from_lsn, @to_lsn, lower(rtrim(ltrim(@row_filter_option))), 0) = 1)
		and (t.__$operation = 1 or t.__$operation = 2 or t.__$operation = 4)
		and (t.__$start_lsn <= @to_lsn)
		and (t.__$start_lsn >= @from_lsn)
		
	union all	
		
	select t.__$start_lsn as __$start_lsn,
		t.__$seqval as __$seqval,
		t.__$operation as __$operation,
		t.__$update_mask as __$update_mask, t.[AccountNo], t.[TMSCounterparty], t.[UltracsCounterparty], t.[ClientType], t.[RBNZResidencyMapping1]
	from [cdc].[dbo_MRU_TMS_Client_Mapping_CT] t with (nolock)     
	where (lower(rtrim(ltrim(@row_filter_option))) = 'all update old')
	    and ( [sys].[fn_cdc_check_parameters]( N'dbo_MRU_TMS_Client_Mapping', @from_lsn, @to_lsn, lower(rtrim(ltrim(@row_filter_option))), 0) = 1)
		and (t.__$operation = 1 or t.__$operation = 2 or t.__$operation = 4 or
		     t.__$operation = 3 )
		and (t.__$start_lsn <= @to_lsn)
		and (t.__$start_lsn >= @from_lsn)
	
GO
/****** Object:  UserDefinedFunction [cdc].[fn_cdc_get_all_changes_dbo_MRU_TMS_ClientGroup_Mapping]    Script Date: 2/11/2023 1:33:40 pm ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

	create function [cdc].[fn_cdc_get_all_changes_dbo_MRU_TMS_ClientGroup_Mapping]
	(	@from_lsn binary(10),
		@to_lsn binary(10),
		@row_filter_option nvarchar(30)
	)
	returns table
	return
	
	select NULL as __$start_lsn,
		NULL as __$seqval,
		NULL as __$operation,
		NULL as __$update_mask, NULL as [GroupAccountNo], NULL as [GroupName], NULL as [AccountNo], NULL as [UltracsCounterparty], NULL as [ClientType], NULL as [RBNZResidencyMapping1]
	where ( [sys].[fn_cdc_check_parameters]( N'dbo_MRU_TMS_ClientGroup_Mapping', @from_lsn, @to_lsn, lower(rtrim(ltrim(@row_filter_option))), 0) = 0)

	union all
	
	select t.__$start_lsn as __$start_lsn,
		t.__$seqval as __$seqval,
		t.__$operation as __$operation,
		t.__$update_mask as __$update_mask, t.[GroupAccountNo], t.[GroupName], t.[AccountNo], t.[UltracsCounterparty], t.[ClientType], t.[RBNZResidencyMapping1]
	from [cdc].[dbo_MRU_TMS_ClientGroup_Mapping_CT] t with (nolock)    
	where (lower(rtrim(ltrim(@row_filter_option))) = 'all')
	    and ( [sys].[fn_cdc_check_parameters]( N'dbo_MRU_TMS_ClientGroup_Mapping', @from_lsn, @to_lsn, lower(rtrim(ltrim(@row_filter_option))), 0) = 1)
		and (t.__$operation = 1 or t.__$operation = 2 or t.__$operation = 4)
		and (t.__$start_lsn <= @to_lsn)
		and (t.__$start_lsn >= @from_lsn)
		
	union all	
		
	select t.__$start_lsn as __$start_lsn,
		t.__$seqval as __$seqval,
		t.__$operation as __$operation,
		t.__$update_mask as __$update_mask, t.[GroupAccountNo], t.[GroupName], t.[AccountNo], t.[UltracsCounterparty], t.[ClientType], t.[RBNZResidencyMapping1]
	from [cdc].[dbo_MRU_TMS_ClientGroup_Mapping_CT] t with (nolock)     
	where (lower(rtrim(ltrim(@row_filter_option))) = 'all update old')
	    and ( [sys].[fn_cdc_check_parameters]( N'dbo_MRU_TMS_ClientGroup_Mapping', @from_lsn, @to_lsn, lower(rtrim(ltrim(@row_filter_option))), 0) = 1)
		and (t.__$operation = 1 or t.__$operation = 2 or t.__$operation = 4 or
		     t.__$operation = 3 )
		and (t.__$start_lsn <= @to_lsn)
		and (t.__$start_lsn >= @from_lsn)
	
GO
/****** Object:  UserDefinedFunction [cdc].[fn_cdc_get_all_changes_dbo_MRU_TMS_Deal_Override]    Script Date: 2/11/2023 1:33:40 pm ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

	create function [cdc].[fn_cdc_get_all_changes_dbo_MRU_TMS_Deal_Override]
	(	@from_lsn binary(10),
		@to_lsn binary(10),
		@row_filter_option nvarchar(30)
	)
	returns table
	return
	
	select NULL as __$start_lsn,
		NULL as __$seqval,
		NULL as __$operation,
		NULL as __$update_mask, NULL as [Deal Id], NULL as [Deal No], NULL as [Deal Side], NULL as [Residency], NULL as [Notes]
	where ( [sys].[fn_cdc_check_parameters]( N'dbo_MRU_TMS_Deal_Override', @from_lsn, @to_lsn, lower(rtrim(ltrim(@row_filter_option))), 0) = 0)

	union all
	
	select t.__$start_lsn as __$start_lsn,
		t.__$seqval as __$seqval,
		t.__$operation as __$operation,
		t.__$update_mask as __$update_mask, t.[Deal Id], t.[Deal No], t.[Deal Side], t.[Residency], t.[Notes]
	from [cdc].[dbo_MRU_TMS_Deal_Override_CT] t with (nolock)    
	where (lower(rtrim(ltrim(@row_filter_option))) = 'all')
	    and ( [sys].[fn_cdc_check_parameters]( N'dbo_MRU_TMS_Deal_Override', @from_lsn, @to_lsn, lower(rtrim(ltrim(@row_filter_option))), 0) = 1)
		and (t.__$operation = 1 or t.__$operation = 2 or t.__$operation = 4)
		and (t.__$start_lsn <= @to_lsn)
		and (t.__$start_lsn >= @from_lsn)
		
	union all	
		
	select t.__$start_lsn as __$start_lsn,
		t.__$seqval as __$seqval,
		t.__$operation as __$operation,
		t.__$update_mask as __$update_mask, t.[Deal Id], t.[Deal No], t.[Deal Side], t.[Residency], t.[Notes]
	from [cdc].[dbo_MRU_TMS_Deal_Override_CT] t with (nolock)     
	where (lower(rtrim(ltrim(@row_filter_option))) = 'all update old')
	    and ( [sys].[fn_cdc_check_parameters]( N'dbo_MRU_TMS_Deal_Override', @from_lsn, @to_lsn, lower(rtrim(ltrim(@row_filter_option))), 0) = 1)
		and (t.__$operation = 1 or t.__$operation = 2 or t.__$operation = 4 or
		     t.__$operation = 3 )
		and (t.__$start_lsn <= @to_lsn)
		and (t.__$start_lsn >= @from_lsn)
	
GO
/****** Object:  UserDefinedFunction [cdc].[fn_cdc_get_all_changes_dbo_MRU_TMS_Deal_Override_FLP]    Script Date: 2/11/2023 1:33:40 pm ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

	create function [cdc].[fn_cdc_get_all_changes_dbo_MRU_TMS_Deal_Override_FLP]
	(	@from_lsn binary(10),
		@to_lsn binary(10),
		@row_filter_option nvarchar(30)
	)
	returns table
	return
	
	select NULL as __$start_lsn,
		NULL as __$seqval,
		NULL as __$operation,
		NULL as __$update_mask, NULL as [DealId], NULL as [DealNo], NULL as [DealtDate], NULL as [BeginDate], NULL as [EndDate]
	where ( [sys].[fn_cdc_check_parameters]( N'dbo_MRU_TMS_Deal_Override_FLP', @from_lsn, @to_lsn, lower(rtrim(ltrim(@row_filter_option))), 0) = 0)

	union all
	
	select t.__$start_lsn as __$start_lsn,
		t.__$seqval as __$seqval,
		t.__$operation as __$operation,
		t.__$update_mask as __$update_mask, t.[DealId], t.[DealNo], t.[DealtDate], t.[BeginDate], t.[EndDate]
	from [cdc].[dbo_MRU_TMS_Deal_Override_FLP_CT] t with (nolock)    
	where (lower(rtrim(ltrim(@row_filter_option))) = 'all')
	    and ( [sys].[fn_cdc_check_parameters]( N'dbo_MRU_TMS_Deal_Override_FLP', @from_lsn, @to_lsn, lower(rtrim(ltrim(@row_filter_option))), 0) = 1)
		and (t.__$operation = 1 or t.__$operation = 2 or t.__$operation = 4)
		and (t.__$start_lsn <= @to_lsn)
		and (t.__$start_lsn >= @from_lsn)
		
	union all	
		
	select t.__$start_lsn as __$start_lsn,
		t.__$seqval as __$seqval,
		t.__$operation as __$operation,
		t.__$update_mask as __$update_mask, t.[DealId], t.[DealNo], t.[DealtDate], t.[BeginDate], t.[EndDate]
	from [cdc].[dbo_MRU_TMS_Deal_Override_FLP_CT] t with (nolock)     
	where (lower(rtrim(ltrim(@row_filter_option))) = 'all update old')
	    and ( [sys].[fn_cdc_check_parameters]( N'dbo_MRU_TMS_Deal_Override_FLP', @from_lsn, @to_lsn, lower(rtrim(ltrim(@row_filter_option))), 0) = 1)
		and (t.__$operation = 1 or t.__$operation = 2 or t.__$operation = 4 or
		     t.__$operation = 3 )
		and (t.__$start_lsn <= @to_lsn)
		and (t.__$start_lsn >= @from_lsn)
	
GO
/****** Object:  UserDefinedFunction [cdc].[fn_cdc_get_all_changes_dbo_MRU_TMS_Instrument_Mapping]    Script Date: 2/11/2023 1:33:40 pm ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

	create function [cdc].[fn_cdc_get_all_changes_dbo_MRU_TMS_Instrument_Mapping]
	(	@from_lsn binary(10),
		@to_lsn binary(10),
		@row_filter_option nvarchar(30)
	)
	returns table
	return
	
	select NULL as __$start_lsn,
		NULL as __$seqval,
		NULL as __$operation,
		NULL as __$update_mask, NULL as [SourceInstrumentName], NULL as [ProductGroup]
	where ( [sys].[fn_cdc_check_parameters]( N'dbo_MRU_TMS_Instrument_Mapping', @from_lsn, @to_lsn, lower(rtrim(ltrim(@row_filter_option))), 0) = 0)

	union all
	
	select t.__$start_lsn as __$start_lsn,
		t.__$seqval as __$seqval,
		t.__$operation as __$operation,
		t.__$update_mask as __$update_mask, t.[SourceInstrumentName], t.[ProductGroup]
	from [cdc].[dbo_MRU_TMS_Instrument_Mapping_CT] t with (nolock)    
	where (lower(rtrim(ltrim(@row_filter_option))) = 'all')
	    and ( [sys].[fn_cdc_check_parameters]( N'dbo_MRU_TMS_Instrument_Mapping', @from_lsn, @to_lsn, lower(rtrim(ltrim(@row_filter_option))), 0) = 1)
		and (t.__$operation = 1 or t.__$operation = 2 or t.__$operation = 4)
		and (t.__$start_lsn <= @to_lsn)
		and (t.__$start_lsn >= @from_lsn)
		
	union all	
		
	select t.__$start_lsn as __$start_lsn,
		t.__$seqval as __$seqval,
		t.__$operation as __$operation,
		t.__$update_mask as __$update_mask, t.[SourceInstrumentName], t.[ProductGroup]
	from [cdc].[dbo_MRU_TMS_Instrument_Mapping_CT] t with (nolock)     
	where (lower(rtrim(ltrim(@row_filter_option))) = 'all update old')
	    and ( [sys].[fn_cdc_check_parameters]( N'dbo_MRU_TMS_Instrument_Mapping', @from_lsn, @to_lsn, lower(rtrim(ltrim(@row_filter_option))), 0) = 1)
		and (t.__$operation = 1 or t.__$operation = 2 or t.__$operation = 4 or
		     t.__$operation = 3 )
		and (t.__$start_lsn <= @to_lsn)
		and (t.__$start_lsn >= @from_lsn)
	
GO
/****** Object:  UserDefinedFunction [cdc].[fn_cdc_get_net_changes_dbo_MRU_Product_Mapping]    Script Date: 2/11/2023 1:33:40 pm ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

	create function [cdc].[fn_cdc_get_net_changes_dbo_MRU_Product_Mapping]
	(	@from_lsn binary(10),
		@to_lsn binary(10),
		@row_filter_option nvarchar(30)
	)
	returns table
	return

	select NULL as __$start_lsn,
		NULL as __$operation,
		NULL as __$update_mask, NULL as [ProductGrp], NULL as [ProductName], NULL as [ProductType], NULL as [TradeableFlag], NULL as [NetGross], NULL as [Sector], NULL as [NormalState], NULL as [RBNZProductMapping1], NULL as [RBNZProductMapping2]
	where ( [sys].[fn_cdc_check_parameters]( N'dbo_MRU_Product_Mapping', @from_lsn, @to_lsn, lower(rtrim(ltrim(@row_filter_option))), 1) = 0)

	union all
	
	select __$start_lsn,
	    case __$count_B35CC109
	    when 1 then __$operation
	    else
			case __$min_op_B35CC109 
				when 2 then 2
				when 4 then
				case __$operation
					when 1 then 1
					else 4
					end
				else
				case __$operation
					when 2 then 4
					when 4 then 4
					else 1
					end
			end
		end as __$operation,
		null as __$update_mask , [ProductGrp], [ProductName], [ProductType], [TradeableFlag], [NetGross], [Sector], [NormalState], [RBNZProductMapping1], [RBNZProductMapping2]
	from
	(
		select t.__$start_lsn as __$start_lsn, __$operation,
		case __$count_B35CC109 
		when 1 then __$operation 
		else
		(	select top 1 c.__$operation
			from [cdc].[dbo_MRU_Product_Mapping_CT] c with (nolock)   
			where  ( (c.[ProductGrp] = t.[ProductGrp]) )  
			and ((c.__$operation = 2) or (c.__$operation = 4) or (c.__$operation = 1))
			and (c.__$start_lsn <= @to_lsn)
			and (c.__$start_lsn >= @from_lsn)
			order by c.__$start_lsn, c.__$command_id, c.__$seqval) end __$min_op_B35CC109, __$count_B35CC109, t.[ProductGrp], t.[ProductName], t.[ProductType], t.[TradeableFlag], t.[NetGross], t.[Sector], t.[NormalState], t.[RBNZProductMapping1], t.[RBNZProductMapping2] 
		from [cdc].[dbo_MRU_Product_Mapping_CT] t with (nolock) inner join 
		(	select  r.[ProductGrp],
		    count(*) as __$count_B35CC109 
			from [cdc].[dbo_MRU_Product_Mapping_CT] r with (nolock)
			where  (r.__$start_lsn <= @to_lsn)
			and (r.__$start_lsn >= @from_lsn)
			group by   r.[ProductGrp]) m
		on t.__$seqval = ( select top 1 c.__$seqval from [cdc].[dbo_MRU_Product_Mapping_CT] c with (nolock) where  ( (c.[ProductGrp] = t.[ProductGrp]) )  and c.__$start_lsn <= @to_lsn and c.__$start_lsn >= @from_lsn order by c.__$start_lsn desc, c.__$command_id desc, c.__$seqval desc ) and
		    ( (t.[ProductGrp] = m.[ProductGrp]) ) 	
		where lower(rtrim(ltrim(@row_filter_option))) = N'all'
			and ( [sys].[fn_cdc_check_parameters]( N'dbo_MRU_Product_Mapping', @from_lsn, @to_lsn, lower(rtrim(ltrim(@row_filter_option))), 1) = 1)
			and (t.__$start_lsn <= @to_lsn)
			and (t.__$start_lsn >= @from_lsn)
			and ((t.__$operation = 2) or (t.__$operation = 4) or 
				 ((t.__$operation = 1) and
				  (2 not in 
				 		(	select top 1 c.__$operation
							from [cdc].[dbo_MRU_Product_Mapping_CT] c with (nolock) 
							where  ( (c.[ProductGrp] = t.[ProductGrp]) )  
							and ((c.__$operation = 2) or (c.__$operation = 4) or (c.__$operation = 1))
							and (c.__$start_lsn <= @to_lsn)
							and (c.__$start_lsn >= @from_lsn)
							order by c.__$start_lsn, c.__$command_id, c.__$seqval
						 ) 
	 			   )
	 			 )
	 			) 
			and t.__$operation = (
				select
					max(mo.__$operation)
				from
					[cdc].[dbo_MRU_Product_Mapping_CT] as mo with (nolock)
				where
					mo.__$seqval = t.__$seqval
					and 
					 ( (t.[ProductGrp] = mo.[ProductGrp]) ) 
				group by
					mo.__$seqval
			)	
	) Q
	
	union all
	
	select __$start_lsn,
	    case __$count_B35CC109
	    when 1 then __$operation
	    else
			case __$min_op_B35CC109 
				when 2 then 2
				when 4 then
				case __$operation
					when 1 then 1
					else 4
					end
				else
				case __$operation
					when 2 then 4
					when 4 then 4
					else 1
					end
			end
		end as __$operation,
		case __$count_B35CC109
		when 1 then
			case __$operation
			when 4 then __$update_mask
			else null
			end
		else	
			case __$min_op_B35CC109 
			when 2 then null
			else
				case __$operation
				when 1 then null
				else __$update_mask 
				end
			end	
		end as __$update_mask , [ProductGrp], [ProductName], [ProductType], [TradeableFlag], [NetGross], [Sector], [NormalState], [RBNZProductMapping1], [RBNZProductMapping2]
	from
	(
		select t.__$start_lsn as __$start_lsn, __$operation,
		case __$count_B35CC109 
		when 1 then __$operation 
		else
		(	select top 1 c.__$operation
			from [cdc].[dbo_MRU_Product_Mapping_CT] c with (nolock)
			where  ( (c.[ProductGrp] = t.[ProductGrp]) )  
			and ((c.__$operation = 2) or (c.__$operation = 4) or (c.__$operation = 1))
			and (c.__$start_lsn <= @to_lsn)
			and (c.__$start_lsn >= @from_lsn)
			order by c.__$start_lsn, c.__$command_id, c.__$seqval) end __$min_op_B35CC109, __$count_B35CC109, 
		m.__$update_mask , t.[ProductGrp], t.[ProductName], t.[ProductType], t.[TradeableFlag], t.[NetGross], t.[Sector], t.[NormalState], t.[RBNZProductMapping1], t.[RBNZProductMapping2]
		from [cdc].[dbo_MRU_Product_Mapping_CT] t with (nolock) inner join 
		(	select  r.[ProductGrp],
		    count(*) as __$count_B35CC109, 
		    [sys].[ORMask](r.__$update_mask) as __$update_mask
			from [cdc].[dbo_MRU_Product_Mapping_CT] r with (nolock)
			where  (r.__$start_lsn <= @to_lsn)
			and (r.__$start_lsn >= @from_lsn)
			group by   r.[ProductGrp]) m
		on t.__$seqval = ( select top 1 c.__$seqval from [cdc].[dbo_MRU_Product_Mapping_CT] c with (nolock) where  ( (c.[ProductGrp] = t.[ProductGrp]) )  and c.__$start_lsn <= @to_lsn and c.__$start_lsn >= @from_lsn order by c.__$start_lsn desc, c.__$command_id desc, c.__$seqval desc ) and
		    ( (t.[ProductGrp] = m.[ProductGrp]) ) 	
		where lower(rtrim(ltrim(@row_filter_option))) = N'all with mask'
			and ( [sys].[fn_cdc_check_parameters]( N'dbo_MRU_Product_Mapping', @from_lsn, @to_lsn, lower(rtrim(ltrim(@row_filter_option))), 1) = 1)
			and (t.__$start_lsn <= @to_lsn)
			and (t.__$start_lsn >= @from_lsn)
			and ((t.__$operation = 2) or (t.__$operation = 4) or 
				 ((t.__$operation = 1) and
				  (2 not in 
				 		(	select top 1 c.__$operation
							from [cdc].[dbo_MRU_Product_Mapping_CT] c with (nolock)
							where  ( (c.[ProductGrp] = t.[ProductGrp]) )  
							and ((c.__$operation = 2) or (c.__$operation = 4) or (c.__$operation = 1))
							and (c.__$start_lsn <= @to_lsn)
							and (c.__$start_lsn >= @from_lsn)
							order by c.__$start_lsn, c.__$command_id, c.__$seqval
						 ) 
	 			   )
	 			 )
	 			) 
			and t.__$operation = (
				select
					max(mo.__$operation)
				from
					[cdc].[dbo_MRU_Product_Mapping_CT] as mo with (nolock)
				where
					mo.__$seqval = t.__$seqval
					and 
					 ( (t.[ProductGrp] = mo.[ProductGrp]) ) 
				group by
					mo.__$seqval
			)	
	) Q
	
	union all
	
		select t.__$start_lsn as __$start_lsn,
		case t.__$operation
			when 1 then 1
			else 5
		end as __$operation,
		null as __$update_mask , t.[ProductGrp], t.[ProductName], t.[ProductType], t.[TradeableFlag], t.[NetGross], t.[Sector], t.[NormalState], t.[RBNZProductMapping1], t.[RBNZProductMapping2]
		from [cdc].[dbo_MRU_Product_Mapping_CT] t  with (nolock)
		where lower(rtrim(ltrim(@row_filter_option))) = N'all with merge'
			and ( [sys].[fn_cdc_check_parameters]( N'dbo_MRU_Product_Mapping', @from_lsn, @to_lsn, lower(rtrim(ltrim(@row_filter_option))), 1) = 1)
			and (t.__$start_lsn <= @to_lsn)
			and (t.__$start_lsn >= @from_lsn)
			and (t.__$seqval = ( select top 1 c.__$seqval from [cdc].[dbo_MRU_Product_Mapping_CT] c with (nolock) where  ( (c.[ProductGrp] = t.[ProductGrp]) )  and c.__$start_lsn <= @to_lsn and c.__$start_lsn >= @from_lsn order by c.__$start_lsn desc, c.__$command_id desc, c.__$seqval desc ))
			and ((t.__$operation = 2) or (t.__$operation = 4) or 
				 ((t.__$operation = 1) and 
				   (2 not in 
				 		(	select top 1 c.__$operation
							from [cdc].[dbo_MRU_Product_Mapping_CT] c with (nolock)
							where  ( (c.[ProductGrp] = t.[ProductGrp]) )  
							and ((c.__$operation = 2) or (c.__$operation = 4) or (c.__$operation = 1))
							and (c.__$start_lsn <= @to_lsn)
							and (c.__$start_lsn >= @from_lsn)
							order by c.__$start_lsn, c.__$command_id, c.__$seqval
						 ) 
	 				)
	 			 )
	 			)
			and t.__$operation = (
				select
					max(mo.__$operation)
				from
					[cdc].[dbo_MRU_Product_Mapping_CT] as mo with (nolock)
				where
					mo.__$seqval = t.__$seqval
					and 
					 ( (t.[ProductGrp] = mo.[ProductGrp]) ) 
				group by
					mo.__$seqval
			)
	 
GO
/****** Object:  UserDefinedFunction [cdc].[fn_cdc_get_net_changes_dbo_MRU_RBNZ_New_Programme_Issues]    Script Date: 2/11/2023 1:33:40 pm ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

	create function [cdc].[fn_cdc_get_net_changes_dbo_MRU_RBNZ_New_Programme_Issues]
	(	@from_lsn binary(10),
		@to_lsn binary(10),
		@row_filter_option nvarchar(30)
	)
	returns table
	return

	select NULL as __$start_lsn,
		NULL as __$operation,
		NULL as __$update_mask, NULL as [Id], NULL as [Source], NULL as [Identification], NULL as [Product], NULL as [Account No], NULL as [CCY], NULL as [Face Value], NULL as [FXRateTo_NZD], NULL as [Lodgement Date], NULL as [Maturity Date], NULL as [Interpolated_BKBM], NULL as [Margin_BKBM], NULL as [Notes]
	where ( [sys].[fn_cdc_check_parameters]( N'dbo_MRU_RBNZ_New_Programme_Issues', @from_lsn, @to_lsn, lower(rtrim(ltrim(@row_filter_option))), 1) = 0)

	union all
	
	select __$start_lsn,
	    case __$count_4F0A3A81
	    when 1 then __$operation
	    else
			case __$min_op_4F0A3A81 
				when 2 then 2
				when 4 then
				case __$operation
					when 1 then 1
					else 4
					end
				else
				case __$operation
					when 2 then 4
					when 4 then 4
					else 1
					end
			end
		end as __$operation,
		null as __$update_mask , [Id], [Source], [Identification], [Product], [Account No], [CCY], [Face Value], [FXRateTo_NZD], [Lodgement Date], [Maturity Date], [Interpolated_BKBM], [Margin_BKBM], [Notes]
	from
	(
		select t.__$start_lsn as __$start_lsn, __$operation,
		case __$count_4F0A3A81 
		when 1 then __$operation 
		else
		(	select top 1 c.__$operation
			from [cdc].[dbo_MRU_RBNZ_New_Programme_Issues_CT] c with (nolock)   
			where  ( (c.[Id] = t.[Id]) )  
			and ((c.__$operation = 2) or (c.__$operation = 4) or (c.__$operation = 1))
			and (c.__$start_lsn <= @to_lsn)
			and (c.__$start_lsn >= @from_lsn)
			order by c.__$start_lsn, c.__$command_id, c.__$seqval) end __$min_op_4F0A3A81, __$count_4F0A3A81, t.[Id], t.[Source], t.[Identification], t.[Product], t.[Account No], t.[CCY], t.[Face Value], t.[FXRateTo_NZD], t.[Lodgement Date], t.[Maturity Date], t.[Interpolated_BKBM], t.[Margin_BKBM], t.[Notes] 
		from [cdc].[dbo_MRU_RBNZ_New_Programme_Issues_CT] t with (nolock) inner join 
		(	select  r.[Id],
		    count(*) as __$count_4F0A3A81 
			from [cdc].[dbo_MRU_RBNZ_New_Programme_Issues_CT] r with (nolock)
			where  (r.__$start_lsn <= @to_lsn)
			and (r.__$start_lsn >= @from_lsn)
			group by   r.[Id]) m
		on t.__$seqval = ( select top 1 c.__$seqval from [cdc].[dbo_MRU_RBNZ_New_Programme_Issues_CT] c with (nolock) where  ( (c.[Id] = t.[Id]) )  and c.__$start_lsn <= @to_lsn and c.__$start_lsn >= @from_lsn order by c.__$start_lsn desc, c.__$command_id desc, c.__$seqval desc ) and
		    ( (t.[Id] = m.[Id]) ) 	
		where lower(rtrim(ltrim(@row_filter_option))) = N'all'
			and ( [sys].[fn_cdc_check_parameters]( N'dbo_MRU_RBNZ_New_Programme_Issues', @from_lsn, @to_lsn, lower(rtrim(ltrim(@row_filter_option))), 1) = 1)
			and (t.__$start_lsn <= @to_lsn)
			and (t.__$start_lsn >= @from_lsn)
			and ((t.__$operation = 2) or (t.__$operation = 4) or 
				 ((t.__$operation = 1) and
				  (2 not in 
				 		(	select top 1 c.__$operation
							from [cdc].[dbo_MRU_RBNZ_New_Programme_Issues_CT] c with (nolock) 
							where  ( (c.[Id] = t.[Id]) )  
							and ((c.__$operation = 2) or (c.__$operation = 4) or (c.__$operation = 1))
							and (c.__$start_lsn <= @to_lsn)
							and (c.__$start_lsn >= @from_lsn)
							order by c.__$start_lsn, c.__$command_id, c.__$seqval
						 ) 
	 			   )
	 			 )
	 			) 
			and t.__$operation = (
				select
					max(mo.__$operation)
				from
					[cdc].[dbo_MRU_RBNZ_New_Programme_Issues_CT] as mo with (nolock)
				where
					mo.__$seqval = t.__$seqval
					and 
					 ( (t.[Id] = mo.[Id]) ) 
				group by
					mo.__$seqval
			)	
	) Q
	
	union all
	
	select __$start_lsn,
	    case __$count_4F0A3A81
	    when 1 then __$operation
	    else
			case __$min_op_4F0A3A81 
				when 2 then 2
				when 4 then
				case __$operation
					when 1 then 1
					else 4
					end
				else
				case __$operation
					when 2 then 4
					when 4 then 4
					else 1
					end
			end
		end as __$operation,
		case __$count_4F0A3A81
		when 1 then
			case __$operation
			when 4 then __$update_mask
			else null
			end
		else	
			case __$min_op_4F0A3A81 
			when 2 then null
			else
				case __$operation
				when 1 then null
				else __$update_mask 
				end
			end	
		end as __$update_mask , [Id], [Source], [Identification], [Product], [Account No], [CCY], [Face Value], [FXRateTo_NZD], [Lodgement Date], [Maturity Date], [Interpolated_BKBM], [Margin_BKBM], [Notes]
	from
	(
		select t.__$start_lsn as __$start_lsn, __$operation,
		case __$count_4F0A3A81 
		when 1 then __$operation 
		else
		(	select top 1 c.__$operation
			from [cdc].[dbo_MRU_RBNZ_New_Programme_Issues_CT] c with (nolock)
			where  ( (c.[Id] = t.[Id]) )  
			and ((c.__$operation = 2) or (c.__$operation = 4) or (c.__$operation = 1))
			and (c.__$start_lsn <= @to_lsn)
			and (c.__$start_lsn >= @from_lsn)
			order by c.__$start_lsn, c.__$command_id, c.__$seqval) end __$min_op_4F0A3A81, __$count_4F0A3A81, 
		m.__$update_mask , t.[Id], t.[Source], t.[Identification], t.[Product], t.[Account No], t.[CCY], t.[Face Value], t.[FXRateTo_NZD], t.[Lodgement Date], t.[Maturity Date], t.[Interpolated_BKBM], t.[Margin_BKBM], t.[Notes]
		from [cdc].[dbo_MRU_RBNZ_New_Programme_Issues_CT] t with (nolock) inner join 
		(	select  r.[Id],
		    count(*) as __$count_4F0A3A81, 
		    [sys].[ORMask](r.__$update_mask) as __$update_mask
			from [cdc].[dbo_MRU_RBNZ_New_Programme_Issues_CT] r with (nolock)
			where  (r.__$start_lsn <= @to_lsn)
			and (r.__$start_lsn >= @from_lsn)
			group by   r.[Id]) m
		on t.__$seqval = ( select top 1 c.__$seqval from [cdc].[dbo_MRU_RBNZ_New_Programme_Issues_CT] c with (nolock) where  ( (c.[Id] = t.[Id]) )  and c.__$start_lsn <= @to_lsn and c.__$start_lsn >= @from_lsn order by c.__$start_lsn desc, c.__$command_id desc, c.__$seqval desc ) and
		    ( (t.[Id] = m.[Id]) ) 	
		where lower(rtrim(ltrim(@row_filter_option))) = N'all with mask'
			and ( [sys].[fn_cdc_check_parameters]( N'dbo_MRU_RBNZ_New_Programme_Issues', @from_lsn, @to_lsn, lower(rtrim(ltrim(@row_filter_option))), 1) = 1)
			and (t.__$start_lsn <= @to_lsn)
			and (t.__$start_lsn >= @from_lsn)
			and ((t.__$operation = 2) or (t.__$operation = 4) or 
				 ((t.__$operation = 1) and
				  (2 not in 
				 		(	select top 1 c.__$operation
							from [cdc].[dbo_MRU_RBNZ_New_Programme_Issues_CT] c with (nolock)
							where  ( (c.[Id] = t.[Id]) )  
							and ((c.__$operation = 2) or (c.__$operation = 4) or (c.__$operation = 1))
							and (c.__$start_lsn <= @to_lsn)
							and (c.__$start_lsn >= @from_lsn)
							order by c.__$start_lsn, c.__$command_id, c.__$seqval
						 ) 
	 			   )
	 			 )
	 			) 
			and t.__$operation = (
				select
					max(mo.__$operation)
				from
					[cdc].[dbo_MRU_RBNZ_New_Programme_Issues_CT] as mo with (nolock)
				where
					mo.__$seqval = t.__$seqval
					and 
					 ( (t.[Id] = mo.[Id]) ) 
				group by
					mo.__$seqval
			)	
	) Q
	
	union all
	
		select t.__$start_lsn as __$start_lsn,
		case t.__$operation
			when 1 then 1
			else 5
		end as __$operation,
		null as __$update_mask , t.[Id], t.[Source], t.[Identification], t.[Product], t.[Account No], t.[CCY], t.[Face Value], t.[FXRateTo_NZD], t.[Lodgement Date], t.[Maturity Date], t.[Interpolated_BKBM], t.[Margin_BKBM], t.[Notes]
		from [cdc].[dbo_MRU_RBNZ_New_Programme_Issues_CT] t  with (nolock)
		where lower(rtrim(ltrim(@row_filter_option))) = N'all with merge'
			and ( [sys].[fn_cdc_check_parameters]( N'dbo_MRU_RBNZ_New_Programme_Issues', @from_lsn, @to_lsn, lower(rtrim(ltrim(@row_filter_option))), 1) = 1)
			and (t.__$start_lsn <= @to_lsn)
			and (t.__$start_lsn >= @from_lsn)
			and (t.__$seqval = ( select top 1 c.__$seqval from [cdc].[dbo_MRU_RBNZ_New_Programme_Issues_CT] c with (nolock) where  ( (c.[Id] = t.[Id]) )  and c.__$start_lsn <= @to_lsn and c.__$start_lsn >= @from_lsn order by c.__$start_lsn desc, c.__$command_id desc, c.__$seqval desc ))
			and ((t.__$operation = 2) or (t.__$operation = 4) or 
				 ((t.__$operation = 1) and 
				   (2 not in 
				 		(	select top 1 c.__$operation
							from [cdc].[dbo_MRU_RBNZ_New_Programme_Issues_CT] c with (nolock)
							where  ( (c.[Id] = t.[Id]) )  
							and ((c.__$operation = 2) or (c.__$operation = 4) or (c.__$operation = 1))
							and (c.__$start_lsn <= @to_lsn)
							and (c.__$start_lsn >= @from_lsn)
							order by c.__$start_lsn, c.__$command_id, c.__$seqval
						 ) 
	 				)
	 			 )
	 			)
			and t.__$operation = (
				select
					max(mo.__$operation)
				from
					[cdc].[dbo_MRU_RBNZ_New_Programme_Issues_CT] as mo with (nolock)
				where
					mo.__$seqval = t.__$seqval
					and 
					 ( (t.[Id] = mo.[Id]) ) 
				group by
					mo.__$seqval
			)
	 
GO
/****** Object:  UserDefinedFunction [cdc].[fn_cdc_get_net_changes_dbo_MRU_TMS_Client_Mapping]    Script Date: 2/11/2023 1:33:40 pm ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

	create function [cdc].[fn_cdc_get_net_changes_dbo_MRU_TMS_Client_Mapping]
	(	@from_lsn binary(10),
		@to_lsn binary(10),
		@row_filter_option nvarchar(30)
	)
	returns table
	return

	select NULL as __$start_lsn,
		NULL as __$operation,
		NULL as __$update_mask, NULL as [AccountNo], NULL as [TMSCounterparty], NULL as [UltracsCounterparty], NULL as [ClientType], NULL as [RBNZResidencyMapping1]
	where ( [sys].[fn_cdc_check_parameters]( N'dbo_MRU_TMS_Client_Mapping', @from_lsn, @to_lsn, lower(rtrim(ltrim(@row_filter_option))), 1) = 0)

	union all
	
	select __$start_lsn,
	    case __$count_9AE3ACA1
	    when 1 then __$operation
	    else
			case __$min_op_9AE3ACA1 
				when 2 then 2
				when 4 then
				case __$operation
					when 1 then 1
					else 4
					end
				else
				case __$operation
					when 2 then 4
					when 4 then 4
					else 1
					end
			end
		end as __$operation,
		null as __$update_mask , [AccountNo], [TMSCounterparty], [UltracsCounterparty], [ClientType], [RBNZResidencyMapping1]
	from
	(
		select t.__$start_lsn as __$start_lsn, __$operation,
		case __$count_9AE3ACA1 
		when 1 then __$operation 
		else
		(	select top 1 c.__$operation
			from [cdc].[dbo_MRU_TMS_Client_Mapping_CT] c with (nolock)   
			where  ( (c.[AccountNo] = t.[AccountNo]) )  
			and ((c.__$operation = 2) or (c.__$operation = 4) or (c.__$operation = 1))
			and (c.__$start_lsn <= @to_lsn)
			and (c.__$start_lsn >= @from_lsn)
			order by c.__$start_lsn, c.__$command_id, c.__$seqval) end __$min_op_9AE3ACA1, __$count_9AE3ACA1, t.[AccountNo], t.[TMSCounterparty], t.[UltracsCounterparty], t.[ClientType], t.[RBNZResidencyMapping1] 
		from [cdc].[dbo_MRU_TMS_Client_Mapping_CT] t with (nolock) inner join 
		(	select  r.[AccountNo],
		    count(*) as __$count_9AE3ACA1 
			from [cdc].[dbo_MRU_TMS_Client_Mapping_CT] r with (nolock)
			where  (r.__$start_lsn <= @to_lsn)
			and (r.__$start_lsn >= @from_lsn)
			group by   r.[AccountNo]) m
		on t.__$seqval = ( select top 1 c.__$seqval from [cdc].[dbo_MRU_TMS_Client_Mapping_CT] c with (nolock) where  ( (c.[AccountNo] = t.[AccountNo]) )  and c.__$start_lsn <= @to_lsn and c.__$start_lsn >= @from_lsn order by c.__$start_lsn desc, c.__$command_id desc, c.__$seqval desc ) and
		    ( (t.[AccountNo] = m.[AccountNo]) ) 	
		where lower(rtrim(ltrim(@row_filter_option))) = N'all'
			and ( [sys].[fn_cdc_check_parameters]( N'dbo_MRU_TMS_Client_Mapping', @from_lsn, @to_lsn, lower(rtrim(ltrim(@row_filter_option))), 1) = 1)
			and (t.__$start_lsn <= @to_lsn)
			and (t.__$start_lsn >= @from_lsn)
			and ((t.__$operation = 2) or (t.__$operation = 4) or 
				 ((t.__$operation = 1) and
				  (2 not in 
				 		(	select top 1 c.__$operation
							from [cdc].[dbo_MRU_TMS_Client_Mapping_CT] c with (nolock) 
							where  ( (c.[AccountNo] = t.[AccountNo]) )  
							and ((c.__$operation = 2) or (c.__$operation = 4) or (c.__$operation = 1))
							and (c.__$start_lsn <= @to_lsn)
							and (c.__$start_lsn >= @from_lsn)
							order by c.__$start_lsn, c.__$command_id, c.__$seqval
						 ) 
	 			   )
	 			 )
	 			) 
			and t.__$operation = (
				select
					max(mo.__$operation)
				from
					[cdc].[dbo_MRU_TMS_Client_Mapping_CT] as mo with (nolock)
				where
					mo.__$seqval = t.__$seqval
					and 
					 ( (t.[AccountNo] = mo.[AccountNo]) ) 
				group by
					mo.__$seqval
			)	
	) Q
	
	union all
	
	select __$start_lsn,
	    case __$count_9AE3ACA1
	    when 1 then __$operation
	    else
			case __$min_op_9AE3ACA1 
				when 2 then 2
				when 4 then
				case __$operation
					when 1 then 1
					else 4
					end
				else
				case __$operation
					when 2 then 4
					when 4 then 4
					else 1
					end
			end
		end as __$operation,
		case __$count_9AE3ACA1
		when 1 then
			case __$operation
			when 4 then __$update_mask
			else null
			end
		else	
			case __$min_op_9AE3ACA1 
			when 2 then null
			else
				case __$operation
				when 1 then null
				else __$update_mask 
				end
			end	
		end as __$update_mask , [AccountNo], [TMSCounterparty], [UltracsCounterparty], [ClientType], [RBNZResidencyMapping1]
	from
	(
		select t.__$start_lsn as __$start_lsn, __$operation,
		case __$count_9AE3ACA1 
		when 1 then __$operation 
		else
		(	select top 1 c.__$operation
			from [cdc].[dbo_MRU_TMS_Client_Mapping_CT] c with (nolock)
			where  ( (c.[AccountNo] = t.[AccountNo]) )  
			and ((c.__$operation = 2) or (c.__$operation = 4) or (c.__$operation = 1))
			and (c.__$start_lsn <= @to_lsn)
			and (c.__$start_lsn >= @from_lsn)
			order by c.__$start_lsn, c.__$command_id, c.__$seqval) end __$min_op_9AE3ACA1, __$count_9AE3ACA1, 
		m.__$update_mask , t.[AccountNo], t.[TMSCounterparty], t.[UltracsCounterparty], t.[ClientType], t.[RBNZResidencyMapping1]
		from [cdc].[dbo_MRU_TMS_Client_Mapping_CT] t with (nolock) inner join 
		(	select  r.[AccountNo],
		    count(*) as __$count_9AE3ACA1, 
		    [sys].[ORMask](r.__$update_mask) as __$update_mask
			from [cdc].[dbo_MRU_TMS_Client_Mapping_CT] r with (nolock)
			where  (r.__$start_lsn <= @to_lsn)
			and (r.__$start_lsn >= @from_lsn)
			group by   r.[AccountNo]) m
		on t.__$seqval = ( select top 1 c.__$seqval from [cdc].[dbo_MRU_TMS_Client_Mapping_CT] c with (nolock) where  ( (c.[AccountNo] = t.[AccountNo]) )  and c.__$start_lsn <= @to_lsn and c.__$start_lsn >= @from_lsn order by c.__$start_lsn desc, c.__$command_id desc, c.__$seqval desc ) and
		    ( (t.[AccountNo] = m.[AccountNo]) ) 	
		where lower(rtrim(ltrim(@row_filter_option))) = N'all with mask'
			and ( [sys].[fn_cdc_check_parameters]( N'dbo_MRU_TMS_Client_Mapping', @from_lsn, @to_lsn, lower(rtrim(ltrim(@row_filter_option))), 1) = 1)
			and (t.__$start_lsn <= @to_lsn)
			and (t.__$start_lsn >= @from_lsn)
			and ((t.__$operation = 2) or (t.__$operation = 4) or 
				 ((t.__$operation = 1) and
				  (2 not in 
				 		(	select top 1 c.__$operation
							from [cdc].[dbo_MRU_TMS_Client_Mapping_CT] c with (nolock)
							where  ( (c.[AccountNo] = t.[AccountNo]) )  
							and ((c.__$operation = 2) or (c.__$operation = 4) or (c.__$operation = 1))
							and (c.__$start_lsn <= @to_lsn)
							and (c.__$start_lsn >= @from_lsn)
							order by c.__$start_lsn, c.__$command_id, c.__$seqval
						 ) 
	 			   )
	 			 )
	 			) 
			and t.__$operation = (
				select
					max(mo.__$operation)
				from
					[cdc].[dbo_MRU_TMS_Client_Mapping_CT] as mo with (nolock)
				where
					mo.__$seqval = t.__$seqval
					and 
					 ( (t.[AccountNo] = mo.[AccountNo]) ) 
				group by
					mo.__$seqval
			)	
	) Q
	
	union all
	
		select t.__$start_lsn as __$start_lsn,
		case t.__$operation
			when 1 then 1
			else 5
		end as __$operation,
		null as __$update_mask , t.[AccountNo], t.[TMSCounterparty], t.[UltracsCounterparty], t.[ClientType], t.[RBNZResidencyMapping1]
		from [cdc].[dbo_MRU_TMS_Client_Mapping_CT] t  with (nolock)
		where lower(rtrim(ltrim(@row_filter_option))) = N'all with merge'
			and ( [sys].[fn_cdc_check_parameters]( N'dbo_MRU_TMS_Client_Mapping', @from_lsn, @to_lsn, lower(rtrim(ltrim(@row_filter_option))), 1) = 1)
			and (t.__$start_lsn <= @to_lsn)
			and (t.__$start_lsn >= @from_lsn)
			and (t.__$seqval = ( select top 1 c.__$seqval from [cdc].[dbo_MRU_TMS_Client_Mapping_CT] c with (nolock) where  ( (c.[AccountNo] = t.[AccountNo]) )  and c.__$start_lsn <= @to_lsn and c.__$start_lsn >= @from_lsn order by c.__$start_lsn desc, c.__$command_id desc, c.__$seqval desc ))
			and ((t.__$operation = 2) or (t.__$operation = 4) or 
				 ((t.__$operation = 1) and 
				   (2 not in 
				 		(	select top 1 c.__$operation
							from [cdc].[dbo_MRU_TMS_Client_Mapping_CT] c with (nolock)
							where  ( (c.[AccountNo] = t.[AccountNo]) )  
							and ((c.__$operation = 2) or (c.__$operation = 4) or (c.__$operation = 1))
							and (c.__$start_lsn <= @to_lsn)
							and (c.__$start_lsn >= @from_lsn)
							order by c.__$start_lsn, c.__$command_id, c.__$seqval
						 ) 
	 				)
	 			 )
	 			)
			and t.__$operation = (
				select
					max(mo.__$operation)
				from
					[cdc].[dbo_MRU_TMS_Client_Mapping_CT] as mo with (nolock)
				where
					mo.__$seqval = t.__$seqval
					and 
					 ( (t.[AccountNo] = mo.[AccountNo]) ) 
				group by
					mo.__$seqval
			)
	 
GO
/****** Object:  UserDefinedFunction [cdc].[fn_cdc_get_net_changes_dbo_MRU_TMS_ClientGroup_Mapping]    Script Date: 2/11/2023 1:33:40 pm ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

	create function [cdc].[fn_cdc_get_net_changes_dbo_MRU_TMS_ClientGroup_Mapping]
	(	@from_lsn binary(10),
		@to_lsn binary(10),
		@row_filter_option nvarchar(30)
	)
	returns table
	return

	select NULL as __$start_lsn,
		NULL as __$operation,
		NULL as __$update_mask, NULL as [GroupAccountNo], NULL as [GroupName], NULL as [AccountNo], NULL as [UltracsCounterparty], NULL as [ClientType], NULL as [RBNZResidencyMapping1]
	where ( [sys].[fn_cdc_check_parameters]( N'dbo_MRU_TMS_ClientGroup_Mapping', @from_lsn, @to_lsn, lower(rtrim(ltrim(@row_filter_option))), 1) = 0)

	union all
	
	select __$start_lsn,
	    case __$count_27E4D8EB
	    when 1 then __$operation
	    else
			case __$min_op_27E4D8EB 
				when 2 then 2
				when 4 then
				case __$operation
					when 1 then 1
					else 4
					end
				else
				case __$operation
					when 2 then 4
					when 4 then 4
					else 1
					end
			end
		end as __$operation,
		null as __$update_mask , [GroupAccountNo], [GroupName], [AccountNo], [UltracsCounterparty], [ClientType], [RBNZResidencyMapping1]
	from
	(
		select t.__$start_lsn as __$start_lsn, __$operation,
		case __$count_27E4D8EB 
		when 1 then __$operation 
		else
		(	select top 1 c.__$operation
			from [cdc].[dbo_MRU_TMS_ClientGroup_Mapping_CT] c with (nolock)   
			where  ( (c.[GroupAccountNo] = t.[GroupAccountNo]) and (c.[AccountNo] = t.[AccountNo])  )  
			and ((c.__$operation = 2) or (c.__$operation = 4) or (c.__$operation = 1))
			and (c.__$start_lsn <= @to_lsn)
			and (c.__$start_lsn >= @from_lsn)
			order by c.__$start_lsn, c.__$command_id, c.__$seqval) end __$min_op_27E4D8EB, __$count_27E4D8EB, t.[GroupAccountNo], t.[GroupName], t.[AccountNo], t.[UltracsCounterparty], t.[ClientType], t.[RBNZResidencyMapping1] 
		from [cdc].[dbo_MRU_TMS_ClientGroup_Mapping_CT] t with (nolock) inner join 
		(	select  r.[GroupAccountNo], r.[AccountNo],
		    count(*) as __$count_27E4D8EB 
			from [cdc].[dbo_MRU_TMS_ClientGroup_Mapping_CT] r with (nolock)
			where  (r.__$start_lsn <= @to_lsn)
			and (r.__$start_lsn >= @from_lsn)
			group by   r.[GroupAccountNo], r.[AccountNo]) m
		on t.__$seqval = ( select top 1 c.__$seqval from [cdc].[dbo_MRU_TMS_ClientGroup_Mapping_CT] c with (nolock) where  ( (c.[GroupAccountNo] = t.[GroupAccountNo]) and (c.[AccountNo] = t.[AccountNo])  )  and c.__$start_lsn <= @to_lsn and c.__$start_lsn >= @from_lsn order by c.__$start_lsn desc, c.__$command_id desc, c.__$seqval desc ) and
		    ( (t.[GroupAccountNo] = m.[GroupAccountNo]) and (t.[AccountNo] = m.[AccountNo])  ) 	
		where lower(rtrim(ltrim(@row_filter_option))) = N'all'
			and ( [sys].[fn_cdc_check_parameters]( N'dbo_MRU_TMS_ClientGroup_Mapping', @from_lsn, @to_lsn, lower(rtrim(ltrim(@row_filter_option))), 1) = 1)
			and (t.__$start_lsn <= @to_lsn)
			and (t.__$start_lsn >= @from_lsn)
			and ((t.__$operation = 2) or (t.__$operation = 4) or 
				 ((t.__$operation = 1) and
				  (2 not in 
				 		(	select top 1 c.__$operation
							from [cdc].[dbo_MRU_TMS_ClientGroup_Mapping_CT] c with (nolock) 
							where  ( (c.[GroupAccountNo] = t.[GroupAccountNo]) and (c.[AccountNo] = t.[AccountNo])  )  
							and ((c.__$operation = 2) or (c.__$operation = 4) or (c.__$operation = 1))
							and (c.__$start_lsn <= @to_lsn)
							and (c.__$start_lsn >= @from_lsn)
							order by c.__$start_lsn, c.__$command_id, c.__$seqval
						 ) 
	 			   )
	 			 )
	 			) 
			and t.__$operation = (
				select
					max(mo.__$operation)
				from
					[cdc].[dbo_MRU_TMS_ClientGroup_Mapping_CT] as mo with (nolock)
				where
					mo.__$seqval = t.__$seqval
					and 
					 ( (t.[GroupAccountNo] = mo.[GroupAccountNo]) and (t.[AccountNo] = mo.[AccountNo])  ) 
				group by
					mo.__$seqval
			)	
	) Q
	
	union all
	
	select __$start_lsn,
	    case __$count_27E4D8EB
	    when 1 then __$operation
	    else
			case __$min_op_27E4D8EB 
				when 2 then 2
				when 4 then
				case __$operation
					when 1 then 1
					else 4
					end
				else
				case __$operation
					when 2 then 4
					when 4 then 4
					else 1
					end
			end
		end as __$operation,
		case __$count_27E4D8EB
		when 1 then
			case __$operation
			when 4 then __$update_mask
			else null
			end
		else	
			case __$min_op_27E4D8EB 
			when 2 then null
			else
				case __$operation
				when 1 then null
				else __$update_mask 
				end
			end	
		end as __$update_mask , [GroupAccountNo], [GroupName], [AccountNo], [UltracsCounterparty], [ClientType], [RBNZResidencyMapping1]
	from
	(
		select t.__$start_lsn as __$start_lsn, __$operation,
		case __$count_27E4D8EB 
		when 1 then __$operation 
		else
		(	select top 1 c.__$operation
			from [cdc].[dbo_MRU_TMS_ClientGroup_Mapping_CT] c with (nolock)
			where  ( (c.[GroupAccountNo] = t.[GroupAccountNo]) and (c.[AccountNo] = t.[AccountNo])  )  
			and ((c.__$operation = 2) or (c.__$operation = 4) or (c.__$operation = 1))
			and (c.__$start_lsn <= @to_lsn)
			and (c.__$start_lsn >= @from_lsn)
			order by c.__$start_lsn, c.__$command_id, c.__$seqval) end __$min_op_27E4D8EB, __$count_27E4D8EB, 
		m.__$update_mask , t.[GroupAccountNo], t.[GroupName], t.[AccountNo], t.[UltracsCounterparty], t.[ClientType], t.[RBNZResidencyMapping1]
		from [cdc].[dbo_MRU_TMS_ClientGroup_Mapping_CT] t with (nolock) inner join 
		(	select  r.[GroupAccountNo], r.[AccountNo],
		    count(*) as __$count_27E4D8EB, 
		    [sys].[ORMask](r.__$update_mask) as __$update_mask
			from [cdc].[dbo_MRU_TMS_ClientGroup_Mapping_CT] r with (nolock)
			where  (r.__$start_lsn <= @to_lsn)
			and (r.__$start_lsn >= @from_lsn)
			group by   r.[GroupAccountNo], r.[AccountNo]) m
		on t.__$seqval = ( select top 1 c.__$seqval from [cdc].[dbo_MRU_TMS_ClientGroup_Mapping_CT] c with (nolock) where  ( (c.[GroupAccountNo] = t.[GroupAccountNo]) and (c.[AccountNo] = t.[AccountNo])  )  and c.__$start_lsn <= @to_lsn and c.__$start_lsn >= @from_lsn order by c.__$start_lsn desc, c.__$command_id desc, c.__$seqval desc ) and
		    ( (t.[GroupAccountNo] = m.[GroupAccountNo]) and (t.[AccountNo] = m.[AccountNo])  ) 	
		where lower(rtrim(ltrim(@row_filter_option))) = N'all with mask'
			and ( [sys].[fn_cdc_check_parameters]( N'dbo_MRU_TMS_ClientGroup_Mapping', @from_lsn, @to_lsn, lower(rtrim(ltrim(@row_filter_option))), 1) = 1)
			and (t.__$start_lsn <= @to_lsn)
			and (t.__$start_lsn >= @from_lsn)
			and ((t.__$operation = 2) or (t.__$operation = 4) or 
				 ((t.__$operation = 1) and
				  (2 not in 
				 		(	select top 1 c.__$operation
							from [cdc].[dbo_MRU_TMS_ClientGroup_Mapping_CT] c with (nolock)
							where  ( (c.[GroupAccountNo] = t.[GroupAccountNo]) and (c.[AccountNo] = t.[AccountNo])  )  
							and ((c.__$operation = 2) or (c.__$operation = 4) or (c.__$operation = 1))
							and (c.__$start_lsn <= @to_lsn)
							and (c.__$start_lsn >= @from_lsn)
							order by c.__$start_lsn, c.__$command_id, c.__$seqval
						 ) 
	 			   )
	 			 )
	 			) 
			and t.__$operation = (
				select
					max(mo.__$operation)
				from
					[cdc].[dbo_MRU_TMS_ClientGroup_Mapping_CT] as mo with (nolock)
				where
					mo.__$seqval = t.__$seqval
					and 
					 ( (t.[GroupAccountNo] = mo.[GroupAccountNo]) and (t.[AccountNo] = mo.[AccountNo])  ) 
				group by
					mo.__$seqval
			)	
	) Q
	
	union all
	
		select t.__$start_lsn as __$start_lsn,
		case t.__$operation
			when 1 then 1
			else 5
		end as __$operation,
		null as __$update_mask , t.[GroupAccountNo], t.[GroupName], t.[AccountNo], t.[UltracsCounterparty], t.[ClientType], t.[RBNZResidencyMapping1]
		from [cdc].[dbo_MRU_TMS_ClientGroup_Mapping_CT] t  with (nolock)
		where lower(rtrim(ltrim(@row_filter_option))) = N'all with merge'
			and ( [sys].[fn_cdc_check_parameters]( N'dbo_MRU_TMS_ClientGroup_Mapping', @from_lsn, @to_lsn, lower(rtrim(ltrim(@row_filter_option))), 1) = 1)
			and (t.__$start_lsn <= @to_lsn)
			and (t.__$start_lsn >= @from_lsn)
			and (t.__$seqval = ( select top 1 c.__$seqval from [cdc].[dbo_MRU_TMS_ClientGroup_Mapping_CT] c with (nolock) where  ( (c.[GroupAccountNo] = t.[GroupAccountNo]) and (c.[AccountNo] = t.[AccountNo])  )  and c.__$start_lsn <= @to_lsn and c.__$start_lsn >= @from_lsn order by c.__$start_lsn desc, c.__$command_id desc, c.__$seqval desc ))
			and ((t.__$operation = 2) or (t.__$operation = 4) or 
				 ((t.__$operation = 1) and 
				   (2 not in 
				 		(	select top 1 c.__$operation
							from [cdc].[dbo_MRU_TMS_ClientGroup_Mapping_CT] c with (nolock)
							where  ( (c.[GroupAccountNo] = t.[GroupAccountNo]) and (c.[AccountNo] = t.[AccountNo])  )  
							and ((c.__$operation = 2) or (c.__$operation = 4) or (c.__$operation = 1))
							and (c.__$start_lsn <= @to_lsn)
							and (c.__$start_lsn >= @from_lsn)
							order by c.__$start_lsn, c.__$command_id, c.__$seqval
						 ) 
	 				)
	 			 )
	 			)
			and t.__$operation = (
				select
					max(mo.__$operation)
				from
					[cdc].[dbo_MRU_TMS_ClientGroup_Mapping_CT] as mo with (nolock)
				where
					mo.__$seqval = t.__$seqval
					and 
					 ( (t.[GroupAccountNo] = mo.[GroupAccountNo]) and (t.[AccountNo] = mo.[AccountNo])  ) 
				group by
					mo.__$seqval
			)
	 
GO
/****** Object:  UserDefinedFunction [cdc].[fn_cdc_get_net_changes_dbo_MRU_TMS_Deal_Override]    Script Date: 2/11/2023 1:33:40 pm ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

	create function [cdc].[fn_cdc_get_net_changes_dbo_MRU_TMS_Deal_Override]
	(	@from_lsn binary(10),
		@to_lsn binary(10),
		@row_filter_option nvarchar(30)
	)
	returns table
	return

	select NULL as __$start_lsn,
		NULL as __$operation,
		NULL as __$update_mask, NULL as [Deal Id], NULL as [Deal No], NULL as [Deal Side], NULL as [Residency], NULL as [Notes]
	where ( [sys].[fn_cdc_check_parameters]( N'dbo_MRU_TMS_Deal_Override', @from_lsn, @to_lsn, lower(rtrim(ltrim(@row_filter_option))), 1) = 0)

	union all
	
	select __$start_lsn,
	    case __$count_42C27282
	    when 1 then __$operation
	    else
			case __$min_op_42C27282 
				when 2 then 2
				when 4 then
				case __$operation
					when 1 then 1
					else 4
					end
				else
				case __$operation
					when 2 then 4
					when 4 then 4
					else 1
					end
			end
		end as __$operation,
		null as __$update_mask , [Deal Id], [Deal No], [Deal Side], [Residency], [Notes]
	from
	(
		select t.__$start_lsn as __$start_lsn, __$operation,
		case __$count_42C27282 
		when 1 then __$operation 
		else
		(	select top 1 c.__$operation
			from [cdc].[dbo_MRU_TMS_Deal_Override_CT] c with (nolock)   
			where  ( (c.[Deal Id] = t.[Deal Id]) and (c.[Deal No] = t.[Deal No])  and (c.[Deal Side] = t.[Deal Side])  )  
			and ((c.__$operation = 2) or (c.__$operation = 4) or (c.__$operation = 1))
			and (c.__$start_lsn <= @to_lsn)
			and (c.__$start_lsn >= @from_lsn)
			order by c.__$start_lsn, c.__$command_id, c.__$seqval) end __$min_op_42C27282, __$count_42C27282, t.[Deal Id], t.[Deal No], t.[Deal Side], t.[Residency], t.[Notes] 
		from [cdc].[dbo_MRU_TMS_Deal_Override_CT] t with (nolock) inner join 
		(	select  r.[Deal Id], r.[Deal No], r.[Deal Side],
		    count(*) as __$count_42C27282 
			from [cdc].[dbo_MRU_TMS_Deal_Override_CT] r with (nolock)
			where  (r.__$start_lsn <= @to_lsn)
			and (r.__$start_lsn >= @from_lsn)
			group by   r.[Deal Id], r.[Deal No], r.[Deal Side]) m
		on t.__$seqval = ( select top 1 c.__$seqval from [cdc].[dbo_MRU_TMS_Deal_Override_CT] c with (nolock) where  ( (c.[Deal Id] = t.[Deal Id]) and (c.[Deal No] = t.[Deal No])  and (c.[Deal Side] = t.[Deal Side])  )  and c.__$start_lsn <= @to_lsn and c.__$start_lsn >= @from_lsn order by c.__$start_lsn desc, c.__$command_id desc, c.__$seqval desc ) and
		    ( (t.[Deal Id] = m.[Deal Id]) and (t.[Deal No] = m.[Deal No])  and (t.[Deal Side] = m.[Deal Side])  ) 	
		where lower(rtrim(ltrim(@row_filter_option))) = N'all'
			and ( [sys].[fn_cdc_check_parameters]( N'dbo_MRU_TMS_Deal_Override', @from_lsn, @to_lsn, lower(rtrim(ltrim(@row_filter_option))), 1) = 1)
			and (t.__$start_lsn <= @to_lsn)
			and (t.__$start_lsn >= @from_lsn)
			and ((t.__$operation = 2) or (t.__$operation = 4) or 
				 ((t.__$operation = 1) and
				  (2 not in 
				 		(	select top 1 c.__$operation
							from [cdc].[dbo_MRU_TMS_Deal_Override_CT] c with (nolock) 
							where  ( (c.[Deal Id] = t.[Deal Id]) and (c.[Deal No] = t.[Deal No])  and (c.[Deal Side] = t.[Deal Side])  )  
							and ((c.__$operation = 2) or (c.__$operation = 4) or (c.__$operation = 1))
							and (c.__$start_lsn <= @to_lsn)
							and (c.__$start_lsn >= @from_lsn)
							order by c.__$start_lsn, c.__$command_id, c.__$seqval
						 ) 
	 			   )
	 			 )
	 			) 
			and t.__$operation = (
				select
					max(mo.__$operation)
				from
					[cdc].[dbo_MRU_TMS_Deal_Override_CT] as mo with (nolock)
				where
					mo.__$seqval = t.__$seqval
					and 
					 ( (t.[Deal Id] = mo.[Deal Id]) and (t.[Deal No] = mo.[Deal No])  and (t.[Deal Side] = mo.[Deal Side])  ) 
				group by
					mo.__$seqval
			)	
	) Q
	
	union all
	
	select __$start_lsn,
	    case __$count_42C27282
	    when 1 then __$operation
	    else
			case __$min_op_42C27282 
				when 2 then 2
				when 4 then
				case __$operation
					when 1 then 1
					else 4
					end
				else
				case __$operation
					when 2 then 4
					when 4 then 4
					else 1
					end
			end
		end as __$operation,
		case __$count_42C27282
		when 1 then
			case __$operation
			when 4 then __$update_mask
			else null
			end
		else	
			case __$min_op_42C27282 
			when 2 then null
			else
				case __$operation
				when 1 then null
				else __$update_mask 
				end
			end	
		end as __$update_mask , [Deal Id], [Deal No], [Deal Side], [Residency], [Notes]
	from
	(
		select t.__$start_lsn as __$start_lsn, __$operation,
		case __$count_42C27282 
		when 1 then __$operation 
		else
		(	select top 1 c.__$operation
			from [cdc].[dbo_MRU_TMS_Deal_Override_CT] c with (nolock)
			where  ( (c.[Deal Id] = t.[Deal Id]) and (c.[Deal No] = t.[Deal No])  and (c.[Deal Side] = t.[Deal Side])  )  
			and ((c.__$operation = 2) or (c.__$operation = 4) or (c.__$operation = 1))
			and (c.__$start_lsn <= @to_lsn)
			and (c.__$start_lsn >= @from_lsn)
			order by c.__$start_lsn, c.__$command_id, c.__$seqval) end __$min_op_42C27282, __$count_42C27282, 
		m.__$update_mask , t.[Deal Id], t.[Deal No], t.[Deal Side], t.[Residency], t.[Notes]
		from [cdc].[dbo_MRU_TMS_Deal_Override_CT] t with (nolock) inner join 
		(	select  r.[Deal Id], r.[Deal No], r.[Deal Side],
		    count(*) as __$count_42C27282, 
		    [sys].[ORMask](r.__$update_mask) as __$update_mask
			from [cdc].[dbo_MRU_TMS_Deal_Override_CT] r with (nolock)
			where  (r.__$start_lsn <= @to_lsn)
			and (r.__$start_lsn >= @from_lsn)
			group by   r.[Deal Id], r.[Deal No], r.[Deal Side]) m
		on t.__$seqval = ( select top 1 c.__$seqval from [cdc].[dbo_MRU_TMS_Deal_Override_CT] c with (nolock) where  ( (c.[Deal Id] = t.[Deal Id]) and (c.[Deal No] = t.[Deal No])  and (c.[Deal Side] = t.[Deal Side])  )  and c.__$start_lsn <= @to_lsn and c.__$start_lsn >= @from_lsn order by c.__$start_lsn desc, c.__$command_id desc, c.__$seqval desc ) and
		    ( (t.[Deal Id] = m.[Deal Id]) and (t.[Deal No] = m.[Deal No])  and (t.[Deal Side] = m.[Deal Side])  ) 	
		where lower(rtrim(ltrim(@row_filter_option))) = N'all with mask'
			and ( [sys].[fn_cdc_check_parameters]( N'dbo_MRU_TMS_Deal_Override', @from_lsn, @to_lsn, lower(rtrim(ltrim(@row_filter_option))), 1) = 1)
			and (t.__$start_lsn <= @to_lsn)
			and (t.__$start_lsn >= @from_lsn)
			and ((t.__$operation = 2) or (t.__$operation = 4) or 
				 ((t.__$operation = 1) and
				  (2 not in 
				 		(	select top 1 c.__$operation
							from [cdc].[dbo_MRU_TMS_Deal_Override_CT] c with (nolock)
							where  ( (c.[Deal Id] = t.[Deal Id]) and (c.[Deal No] = t.[Deal No])  and (c.[Deal Side] = t.[Deal Side])  )  
							and ((c.__$operation = 2) or (c.__$operation = 4) or (c.__$operation = 1))
							and (c.__$start_lsn <= @to_lsn)
							and (c.__$start_lsn >= @from_lsn)
							order by c.__$start_lsn, c.__$command_id, c.__$seqval
						 ) 
	 			   )
	 			 )
	 			) 
			and t.__$operation = (
				select
					max(mo.__$operation)
				from
					[cdc].[dbo_MRU_TMS_Deal_Override_CT] as mo with (nolock)
				where
					mo.__$seqval = t.__$seqval
					and 
					 ( (t.[Deal Id] = mo.[Deal Id]) and (t.[Deal No] = mo.[Deal No])  and (t.[Deal Side] = mo.[Deal Side])  ) 
				group by
					mo.__$seqval
			)	
	) Q
	
	union all
	
		select t.__$start_lsn as __$start_lsn,
		case t.__$operation
			when 1 then 1
			else 5
		end as __$operation,
		null as __$update_mask , t.[Deal Id], t.[Deal No], t.[Deal Side], t.[Residency], t.[Notes]
		from [cdc].[dbo_MRU_TMS_Deal_Override_CT] t  with (nolock)
		where lower(rtrim(ltrim(@row_filter_option))) = N'all with merge'
			and ( [sys].[fn_cdc_check_parameters]( N'dbo_MRU_TMS_Deal_Override', @from_lsn, @to_lsn, lower(rtrim(ltrim(@row_filter_option))), 1) = 1)
			and (t.__$start_lsn <= @to_lsn)
			and (t.__$start_lsn >= @from_lsn)
			and (t.__$seqval = ( select top 1 c.__$seqval from [cdc].[dbo_MRU_TMS_Deal_Override_CT] c with (nolock) where  ( (c.[Deal Id] = t.[Deal Id]) and (c.[Deal No] = t.[Deal No])  and (c.[Deal Side] = t.[Deal Side])  )  and c.__$start_lsn <= @to_lsn and c.__$start_lsn >= @from_lsn order by c.__$start_lsn desc, c.__$command_id desc, c.__$seqval desc ))
			and ((t.__$operation = 2) or (t.__$operation = 4) or 
				 ((t.__$operation = 1) and 
				   (2 not in 
				 		(	select top 1 c.__$operation
							from [cdc].[dbo_MRU_TMS_Deal_Override_CT] c with (nolock)
							where  ( (c.[Deal Id] = t.[Deal Id]) and (c.[Deal No] = t.[Deal No])  and (c.[Deal Side] = t.[Deal Side])  )  
							and ((c.__$operation = 2) or (c.__$operation = 4) or (c.__$operation = 1))
							and (c.__$start_lsn <= @to_lsn)
							and (c.__$start_lsn >= @from_lsn)
							order by c.__$start_lsn, c.__$command_id, c.__$seqval
						 ) 
	 				)
	 			 )
	 			)
			and t.__$operation = (
				select
					max(mo.__$operation)
				from
					[cdc].[dbo_MRU_TMS_Deal_Override_CT] as mo with (nolock)
				where
					mo.__$seqval = t.__$seqval
					and 
					 ( (t.[Deal Id] = mo.[Deal Id]) and (t.[Deal No] = mo.[Deal No])  and (t.[Deal Side] = mo.[Deal Side])  ) 
				group by
					mo.__$seqval
			)
	 
GO
/****** Object:  UserDefinedFunction [cdc].[fn_cdc_get_net_changes_dbo_MRU_TMS_Deal_Override_FLP]    Script Date: 2/11/2023 1:33:40 pm ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

	create function [cdc].[fn_cdc_get_net_changes_dbo_MRU_TMS_Deal_Override_FLP]
	(	@from_lsn binary(10),
		@to_lsn binary(10),
		@row_filter_option nvarchar(30)
	)
	returns table
	return

	select NULL as __$start_lsn,
		NULL as __$operation,
		NULL as __$update_mask, NULL as [DealId], NULL as [DealNo], NULL as [DealtDate], NULL as [BeginDate], NULL as [EndDate]
	where ( [sys].[fn_cdc_check_parameters]( N'dbo_MRU_TMS_Deal_Override_FLP', @from_lsn, @to_lsn, lower(rtrim(ltrim(@row_filter_option))), 1) = 0)

	union all
	
	select __$start_lsn,
	    case __$count_6961E37D
	    when 1 then __$operation
	    else
			case __$min_op_6961E37D 
				when 2 then 2
				when 4 then
				case __$operation
					when 1 then 1
					else 4
					end
				else
				case __$operation
					when 2 then 4
					when 4 then 4
					else 1
					end
			end
		end as __$operation,
		null as __$update_mask , [DealId], [DealNo], [DealtDate], [BeginDate], [EndDate]
	from
	(
		select t.__$start_lsn as __$start_lsn, __$operation,
		case __$count_6961E37D 
		when 1 then __$operation 
		else
		(	select top 1 c.__$operation
			from [cdc].[dbo_MRU_TMS_Deal_Override_FLP_CT] c with (nolock)   
			where  ( (c.[DealId] = t.[DealId]) )  
			and ((c.__$operation = 2) or (c.__$operation = 4) or (c.__$operation = 1))
			and (c.__$start_lsn <= @to_lsn)
			and (c.__$start_lsn >= @from_lsn)
			order by c.__$start_lsn, c.__$command_id, c.__$seqval) end __$min_op_6961E37D, __$count_6961E37D, t.[DealId], t.[DealNo], t.[DealtDate], t.[BeginDate], t.[EndDate] 
		from [cdc].[dbo_MRU_TMS_Deal_Override_FLP_CT] t with (nolock) inner join 
		(	select  r.[DealId],
		    count(*) as __$count_6961E37D 
			from [cdc].[dbo_MRU_TMS_Deal_Override_FLP_CT] r with (nolock)
			where  (r.__$start_lsn <= @to_lsn)
			and (r.__$start_lsn >= @from_lsn)
			group by   r.[DealId]) m
		on t.__$seqval = ( select top 1 c.__$seqval from [cdc].[dbo_MRU_TMS_Deal_Override_FLP_CT] c with (nolock) where  ( (c.[DealId] = t.[DealId]) )  and c.__$start_lsn <= @to_lsn and c.__$start_lsn >= @from_lsn order by c.__$start_lsn desc, c.__$command_id desc, c.__$seqval desc ) and
		    ( (t.[DealId] = m.[DealId]) ) 	
		where lower(rtrim(ltrim(@row_filter_option))) = N'all'
			and ( [sys].[fn_cdc_check_parameters]( N'dbo_MRU_TMS_Deal_Override_FLP', @from_lsn, @to_lsn, lower(rtrim(ltrim(@row_filter_option))), 1) = 1)
			and (t.__$start_lsn <= @to_lsn)
			and (t.__$start_lsn >= @from_lsn)
			and ((t.__$operation = 2) or (t.__$operation = 4) or 
				 ((t.__$operation = 1) and
				  (2 not in 
				 		(	select top 1 c.__$operation
							from [cdc].[dbo_MRU_TMS_Deal_Override_FLP_CT] c with (nolock) 
							where  ( (c.[DealId] = t.[DealId]) )  
							and ((c.__$operation = 2) or (c.__$operation = 4) or (c.__$operation = 1))
							and (c.__$start_lsn <= @to_lsn)
							and (c.__$start_lsn >= @from_lsn)
							order by c.__$start_lsn, c.__$command_id, c.__$seqval
						 ) 
	 			   )
	 			 )
	 			) 
			and t.__$operation = (
				select
					max(mo.__$operation)
				from
					[cdc].[dbo_MRU_TMS_Deal_Override_FLP_CT] as mo with (nolock)
				where
					mo.__$seqval = t.__$seqval
					and 
					 ( (t.[DealId] = mo.[DealId]) ) 
				group by
					mo.__$seqval
			)	
	) Q
	
	union all
	
	select __$start_lsn,
	    case __$count_6961E37D
	    when 1 then __$operation
	    else
			case __$min_op_6961E37D 
				when 2 then 2
				when 4 then
				case __$operation
					when 1 then 1
					else 4
					end
				else
				case __$operation
					when 2 then 4
					when 4 then 4
					else 1
					end
			end
		end as __$operation,
		case __$count_6961E37D
		when 1 then
			case __$operation
			when 4 then __$update_mask
			else null
			end
		else	
			case __$min_op_6961E37D 
			when 2 then null
			else
				case __$operation
				when 1 then null
				else __$update_mask 
				end
			end	
		end as __$update_mask , [DealId], [DealNo], [DealtDate], [BeginDate], [EndDate]
	from
	(
		select t.__$start_lsn as __$start_lsn, __$operation,
		case __$count_6961E37D 
		when 1 then __$operation 
		else
		(	select top 1 c.__$operation
			from [cdc].[dbo_MRU_TMS_Deal_Override_FLP_CT] c with (nolock)
			where  ( (c.[DealId] = t.[DealId]) )  
			and ((c.__$operation = 2) or (c.__$operation = 4) or (c.__$operation = 1))
			and (c.__$start_lsn <= @to_lsn)
			and (c.__$start_lsn >= @from_lsn)
			order by c.__$start_lsn, c.__$command_id, c.__$seqval) end __$min_op_6961E37D, __$count_6961E37D, 
		m.__$update_mask , t.[DealId], t.[DealNo], t.[DealtDate], t.[BeginDate], t.[EndDate]
		from [cdc].[dbo_MRU_TMS_Deal_Override_FLP_CT] t with (nolock) inner join 
		(	select  r.[DealId],
		    count(*) as __$count_6961E37D, 
		    [sys].[ORMask](r.__$update_mask) as __$update_mask
			from [cdc].[dbo_MRU_TMS_Deal_Override_FLP_CT] r with (nolock)
			where  (r.__$start_lsn <= @to_lsn)
			and (r.__$start_lsn >= @from_lsn)
			group by   r.[DealId]) m
		on t.__$seqval = ( select top 1 c.__$seqval from [cdc].[dbo_MRU_TMS_Deal_Override_FLP_CT] c with (nolock) where  ( (c.[DealId] = t.[DealId]) )  and c.__$start_lsn <= @to_lsn and c.__$start_lsn >= @from_lsn order by c.__$start_lsn desc, c.__$command_id desc, c.__$seqval desc ) and
		    ( (t.[DealId] = m.[DealId]) ) 	
		where lower(rtrim(ltrim(@row_filter_option))) = N'all with mask'
			and ( [sys].[fn_cdc_check_parameters]( N'dbo_MRU_TMS_Deal_Override_FLP', @from_lsn, @to_lsn, lower(rtrim(ltrim(@row_filter_option))), 1) = 1)
			and (t.__$start_lsn <= @to_lsn)
			and (t.__$start_lsn >= @from_lsn)
			and ((t.__$operation = 2) or (t.__$operation = 4) or 
				 ((t.__$operation = 1) and
				  (2 not in 
				 		(	select top 1 c.__$operation
							from [cdc].[dbo_MRU_TMS_Deal_Override_FLP_CT] c with (nolock)
							where  ( (c.[DealId] = t.[DealId]) )  
							and ((c.__$operation = 2) or (c.__$operation = 4) or (c.__$operation = 1))
							and (c.__$start_lsn <= @to_lsn)
							and (c.__$start_lsn >= @from_lsn)
							order by c.__$start_lsn, c.__$command_id, c.__$seqval
						 ) 
	 			   )
	 			 )
	 			) 
			and t.__$operation = (
				select
					max(mo.__$operation)
				from
					[cdc].[dbo_MRU_TMS_Deal_Override_FLP_CT] as mo with (nolock)
				where
					mo.__$seqval = t.__$seqval
					and 
					 ( (t.[DealId] = mo.[DealId]) ) 
				group by
					mo.__$seqval
			)	
	) Q
	
	union all
	
		select t.__$start_lsn as __$start_lsn,
		case t.__$operation
			when 1 then 1
			else 5
		end as __$operation,
		null as __$update_mask , t.[DealId], t.[DealNo], t.[DealtDate], t.[BeginDate], t.[EndDate]
		from [cdc].[dbo_MRU_TMS_Deal_Override_FLP_CT] t  with (nolock)
		where lower(rtrim(ltrim(@row_filter_option))) = N'all with merge'
			and ( [sys].[fn_cdc_check_parameters]( N'dbo_MRU_TMS_Deal_Override_FLP', @from_lsn, @to_lsn, lower(rtrim(ltrim(@row_filter_option))), 1) = 1)
			and (t.__$start_lsn <= @to_lsn)
			and (t.__$start_lsn >= @from_lsn)
			and (t.__$seqval = ( select top 1 c.__$seqval from [cdc].[dbo_MRU_TMS_Deal_Override_FLP_CT] c with (nolock) where  ( (c.[DealId] = t.[DealId]) )  and c.__$start_lsn <= @to_lsn and c.__$start_lsn >= @from_lsn order by c.__$start_lsn desc, c.__$command_id desc, c.__$seqval desc ))
			and ((t.__$operation = 2) or (t.__$operation = 4) or 
				 ((t.__$operation = 1) and 
				   (2 not in 
				 		(	select top 1 c.__$operation
							from [cdc].[dbo_MRU_TMS_Deal_Override_FLP_CT] c with (nolock)
							where  ( (c.[DealId] = t.[DealId]) )  
							and ((c.__$operation = 2) or (c.__$operation = 4) or (c.__$operation = 1))
							and (c.__$start_lsn <= @to_lsn)
							and (c.__$start_lsn >= @from_lsn)
							order by c.__$start_lsn, c.__$command_id, c.__$seqval
						 ) 
	 				)
	 			 )
	 			)
			and t.__$operation = (
				select
					max(mo.__$operation)
				from
					[cdc].[dbo_MRU_TMS_Deal_Override_FLP_CT] as mo with (nolock)
				where
					mo.__$seqval = t.__$seqval
					and 
					 ( (t.[DealId] = mo.[DealId]) ) 
				group by
					mo.__$seqval
			)
	 
GO
/****** Object:  UserDefinedFunction [cdc].[fn_cdc_get_net_changes_dbo_MRU_TMS_Instrument_Mapping]    Script Date: 2/11/2023 1:33:40 pm ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

	create function [cdc].[fn_cdc_get_net_changes_dbo_MRU_TMS_Instrument_Mapping]
	(	@from_lsn binary(10),
		@to_lsn binary(10),
		@row_filter_option nvarchar(30)
	)
	returns table
	return

	select NULL as __$start_lsn,
		NULL as __$operation,
		NULL as __$update_mask, NULL as [SourceInstrumentName], NULL as [ProductGroup]
	where ( [sys].[fn_cdc_check_parameters]( N'dbo_MRU_TMS_Instrument_Mapping', @from_lsn, @to_lsn, lower(rtrim(ltrim(@row_filter_option))), 1) = 0)

	union all
	
	select __$start_lsn,
	    case __$count_0B06360D
	    when 1 then __$operation
	    else
			case __$min_op_0B06360D 
				when 2 then 2
				when 4 then
				case __$operation
					when 1 then 1
					else 4
					end
				else
				case __$operation
					when 2 then 4
					when 4 then 4
					else 1
					end
			end
		end as __$operation,
		null as __$update_mask , [SourceInstrumentName], [ProductGroup]
	from
	(
		select t.__$start_lsn as __$start_lsn, __$operation,
		case __$count_0B06360D 
		when 1 then __$operation 
		else
		(	select top 1 c.__$operation
			from [cdc].[dbo_MRU_TMS_Instrument_Mapping_CT] c with (nolock)   
			where  ( (c.[SourceInstrumentName] = t.[SourceInstrumentName]) )  
			and ((c.__$operation = 2) or (c.__$operation = 4) or (c.__$operation = 1))
			and (c.__$start_lsn <= @to_lsn)
			and (c.__$start_lsn >= @from_lsn)
			order by c.__$start_lsn, c.__$command_id, c.__$seqval) end __$min_op_0B06360D, __$count_0B06360D, t.[SourceInstrumentName], t.[ProductGroup] 
		from [cdc].[dbo_MRU_TMS_Instrument_Mapping_CT] t with (nolock) inner join 
		(	select  r.[SourceInstrumentName],
		    count(*) as __$count_0B06360D 
			from [cdc].[dbo_MRU_TMS_Instrument_Mapping_CT] r with (nolock)
			where  (r.__$start_lsn <= @to_lsn)
			and (r.__$start_lsn >= @from_lsn)
			group by   r.[SourceInstrumentName]) m
		on t.__$seqval = ( select top 1 c.__$seqval from [cdc].[dbo_MRU_TMS_Instrument_Mapping_CT] c with (nolock) where  ( (c.[SourceInstrumentName] = t.[SourceInstrumentName]) )  and c.__$start_lsn <= @to_lsn and c.__$start_lsn >= @from_lsn order by c.__$start_lsn desc, c.__$command_id desc, c.__$seqval desc ) and
		    ( (t.[SourceInstrumentName] = m.[SourceInstrumentName]) ) 	
		where lower(rtrim(ltrim(@row_filter_option))) = N'all'
			and ( [sys].[fn_cdc_check_parameters]( N'dbo_MRU_TMS_Instrument_Mapping', @from_lsn, @to_lsn, lower(rtrim(ltrim(@row_filter_option))), 1) = 1)
			and (t.__$start_lsn <= @to_lsn)
			and (t.__$start_lsn >= @from_lsn)
			and ((t.__$operation = 2) or (t.__$operation = 4) or 
				 ((t.__$operation = 1) and
				  (2 not in 
				 		(	select top 1 c.__$operation
							from [cdc].[dbo_MRU_TMS_Instrument_Mapping_CT] c with (nolock) 
							where  ( (c.[SourceInstrumentName] = t.[SourceInstrumentName]) )  
							and ((c.__$operation = 2) or (c.__$operation = 4) or (c.__$operation = 1))
							and (c.__$start_lsn <= @to_lsn)
							and (c.__$start_lsn >= @from_lsn)
							order by c.__$start_lsn, c.__$command_id, c.__$seqval
						 ) 
	 			   )
	 			 )
	 			) 
			and t.__$operation = (
				select
					max(mo.__$operation)
				from
					[cdc].[dbo_MRU_TMS_Instrument_Mapping_CT] as mo with (nolock)
				where
					mo.__$seqval = t.__$seqval
					and 
					 ( (t.[SourceInstrumentName] = mo.[SourceInstrumentName]) ) 
				group by
					mo.__$seqval
			)	
	) Q
	
	union all
	
	select __$start_lsn,
	    case __$count_0B06360D
	    when 1 then __$operation
	    else
			case __$min_op_0B06360D 
				when 2 then 2
				when 4 then
				case __$operation
					when 1 then 1
					else 4
					end
				else
				case __$operation
					when 2 then 4
					when 4 then 4
					else 1
					end
			end
		end as __$operation,
		case __$count_0B06360D
		when 1 then
			case __$operation
			when 4 then __$update_mask
			else null
			end
		else	
			case __$min_op_0B06360D 
			when 2 then null
			else
				case __$operation
				when 1 then null
				else __$update_mask 
				end
			end	
		end as __$update_mask , [SourceInstrumentName], [ProductGroup]
	from
	(
		select t.__$start_lsn as __$start_lsn, __$operation,
		case __$count_0B06360D 
		when 1 then __$operation 
		else
		(	select top 1 c.__$operation
			from [cdc].[dbo_MRU_TMS_Instrument_Mapping_CT] c with (nolock)
			where  ( (c.[SourceInstrumentName] = t.[SourceInstrumentName]) )  
			and ((c.__$operation = 2) or (c.__$operation = 4) or (c.__$operation = 1))
			and (c.__$start_lsn <= @to_lsn)
			and (c.__$start_lsn >= @from_lsn)
			order by c.__$start_lsn, c.__$command_id, c.__$seqval) end __$min_op_0B06360D, __$count_0B06360D, 
		m.__$update_mask , t.[SourceInstrumentName], t.[ProductGroup]
		from [cdc].[dbo_MRU_TMS_Instrument_Mapping_CT] t with (nolock) inner join 
		(	select  r.[SourceInstrumentName],
		    count(*) as __$count_0B06360D, 
		    [sys].[ORMask](r.__$update_mask) as __$update_mask
			from [cdc].[dbo_MRU_TMS_Instrument_Mapping_CT] r with (nolock)
			where  (r.__$start_lsn <= @to_lsn)
			and (r.__$start_lsn >= @from_lsn)
			group by   r.[SourceInstrumentName]) m
		on t.__$seqval = ( select top 1 c.__$seqval from [cdc].[dbo_MRU_TMS_Instrument_Mapping_CT] c with (nolock) where  ( (c.[SourceInstrumentName] = t.[SourceInstrumentName]) )  and c.__$start_lsn <= @to_lsn and c.__$start_lsn >= @from_lsn order by c.__$start_lsn desc, c.__$command_id desc, c.__$seqval desc ) and
		    ( (t.[SourceInstrumentName] = m.[SourceInstrumentName]) ) 	
		where lower(rtrim(ltrim(@row_filter_option))) = N'all with mask'
			and ( [sys].[fn_cdc_check_parameters]( N'dbo_MRU_TMS_Instrument_Mapping', @from_lsn, @to_lsn, lower(rtrim(ltrim(@row_filter_option))), 1) = 1)
			and (t.__$start_lsn <= @to_lsn)
			and (t.__$start_lsn >= @from_lsn)
			and ((t.__$operation = 2) or (t.__$operation = 4) or 
				 ((t.__$operation = 1) and
				  (2 not in 
				 		(	select top 1 c.__$operation
							from [cdc].[dbo_MRU_TMS_Instrument_Mapping_CT] c with (nolock)
							where  ( (c.[SourceInstrumentName] = t.[SourceInstrumentName]) )  
							and ((c.__$operation = 2) or (c.__$operation = 4) or (c.__$operation = 1))
							and (c.__$start_lsn <= @to_lsn)
							and (c.__$start_lsn >= @from_lsn)
							order by c.__$start_lsn, c.__$command_id, c.__$seqval
						 ) 
	 			   )
	 			 )
	 			) 
			and t.__$operation = (
				select
					max(mo.__$operation)
				from
					[cdc].[dbo_MRU_TMS_Instrument_Mapping_CT] as mo with (nolock)
				where
					mo.__$seqval = t.__$seqval
					and 
					 ( (t.[SourceInstrumentName] = mo.[SourceInstrumentName]) ) 
				group by
					mo.__$seqval
			)	
	) Q
	
	union all
	
		select t.__$start_lsn as __$start_lsn,
		case t.__$operation
			when 1 then 1
			else 5
		end as __$operation,
		null as __$update_mask , t.[SourceInstrumentName], t.[ProductGroup]
		from [cdc].[dbo_MRU_TMS_Instrument_Mapping_CT] t  with (nolock)
		where lower(rtrim(ltrim(@row_filter_option))) = N'all with merge'
			and ( [sys].[fn_cdc_check_parameters]( N'dbo_MRU_TMS_Instrument_Mapping', @from_lsn, @to_lsn, lower(rtrim(ltrim(@row_filter_option))), 1) = 1)
			and (t.__$start_lsn <= @to_lsn)
			and (t.__$start_lsn >= @from_lsn)
			and (t.__$seqval = ( select top 1 c.__$seqval from [cdc].[dbo_MRU_TMS_Instrument_Mapping_CT] c with (nolock) where  ( (c.[SourceInstrumentName] = t.[SourceInstrumentName]) )  and c.__$start_lsn <= @to_lsn and c.__$start_lsn >= @from_lsn order by c.__$start_lsn desc, c.__$command_id desc, c.__$seqval desc ))
			and ((t.__$operation = 2) or (t.__$operation = 4) or 
				 ((t.__$operation = 1) and 
				   (2 not in 
				 		(	select top 1 c.__$operation
							from [cdc].[dbo_MRU_TMS_Instrument_Mapping_CT] c with (nolock)
							where  ( (c.[SourceInstrumentName] = t.[SourceInstrumentName]) )  
							and ((c.__$operation = 2) or (c.__$operation = 4) or (c.__$operation = 1))
							and (c.__$start_lsn <= @to_lsn)
							and (c.__$start_lsn >= @from_lsn)
							order by c.__$start_lsn, c.__$command_id, c.__$seqval
						 ) 
	 				)
	 			 )
	 			)
			and t.__$operation = (
				select
					max(mo.__$operation)
				from
					[cdc].[dbo_MRU_TMS_Instrument_Mapping_CT] as mo with (nolock)
				where
					mo.__$seqval = t.__$seqval
					and 
					 ( (t.[SourceInstrumentName] = mo.[SourceInstrumentName]) ) 
				group by
					mo.__$seqval
			)
	 
GO
/****** Object:  View [dbo].[AccountStatic]    Script Date: 2/11/2023 1:33:40 pm ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE VIEW [dbo].[AccountStatic]
AS

SELECT [Account],[Product],[ProductGrp],[AccountName]
FROM  Ultracs.AccountStaticKB

UNION ALL

SELECT [Account],[Product],[ProductGrp],[AccountName]
FROM Ultracs.AccountStaticNZHL

UNION ALL

SELECT [Account],[Product],[ProductGrp],[AccountName]
FROM Ultracs.AccountStaticAMP;
GO
/****** Object:  View [dbo].[BalanceAdjustment]    Script Date: 2/11/2023 1:33:40 pm ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


CREATE VIEW [dbo].[BalanceAdjustment]
AS

/********************************************************************************************************
 * Compares product balances between liquidity and TMS.
 * Some are expected to be different, e.g.,
 * - L%, S41 and S76 because TMS does not import such deals
 * - Credit cards (S71/72/78/83/87) because liquidity shows in-funds balance only
 * 
 * This view is used for operational control purpose and does not interact with daily regulatory calculation.
 *
 ********************************************************************************************************
 *
 * @version 1.0
 * @author	Stephen Chin
 * @date	2020.09.03
 * 
 * - Start version control
 *
 *
*/

SELECT	LIQ.ProductGroup, 
		LIQ.TotalBalance AS LIQBalance, 
		TMS.Balance AS TMSBalance, 
		ISNULL(LIQ.TotalBalance, 0) - ISNULL(TMS.Balance, 0) AS Diff 

FROM	(Select ProductGroup, SUM(Balance) As TotalBalance From MRU_Liquidity_Funding_Balances_Details_Bandings Group By ProductGroup) LIQ
		LEFT JOIN     
		(Select	ProductGroup, 
                CASE WHEN TIM.ProductGroup = 'MMCSH' AND TMS.[Adj Mapping Profile] = 'MMFund' THEN 
					ABS(SUM(CAST(ISNULL(REPLACE(REPLACE(REPLACE([NZD Carrying Value], ')', ''), '(', '-') ,',',''), 0) AS DECIMAL(18,2)))) 
				ELSE
					CASE WHEN TIM.ProductGroup <> 'MMCSH' THEN
						ABS(SUM(CAST(ISNULL(REPLACE(REPLACE(REPLACE([NZD Carrying Value], ')', ''), '(', '-') ,',',''), 0) AS DECIMAL(18,2))))
                    END
				END As Balance, 
                [Transaction Type], 
				[Settle Status]
		From   MRU_TMS_Liquidity_Reporting TMS
               Left Join MRU_TMS_Instrument_Mapping TIM ON TMS.Instrument = TIM.SourceInstrumentName
        Where [Settle Status] = 'Settled' And ProductGroup Not Like 'L%' And [Transaction Type] <> 'CF'
        Group By ProductGroup, [Transaction Type], [Settle Status], [Adj Mapping Profile]) TMS
		ON LIQ.ProductGroup = TMS.ProductGroup

WHERE  LIQ.ProductGroup <> 'MMCSH' OR Balance IS NOT NULL;

GO
/****** Object:  View [dbo].[CustomersBusinessLinks]    Script Date: 2/11/2023 1:33:40 pm ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE VIEW [dbo].[CustomersBusinessLinks]
AS
SELECT LinkType, LinkToAccessNo, Max(AccessNo) AS AccessNo
FROM Ultracs.CustomersBusinessLinks
GROUP BY LinkType, LinkToAccessNo;
GO
/****** Object:  View [dbo].[HistRates]    Script Date: 2/11/2023 1:33:40 pm ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


CREATE VIEW [dbo].[HistRates]
AS

/********************************************************************************************************
 * This view stores interpolated rates of interim pillars based on input table of historical rates,
 * and being used in WAC of new issuance.
 * 
 ********************************************************************************************************
 *
 * @version 1.0
 * @author	Stephen Chin
 * @date	2020.08.13
 * 
 * - Start version control
 *
 * 
*/

SELECT	[Date] AS FundingDate, 
		OCR AS Overnight, 
		[1 Month] AS [30], 
		[2 Month] AS [60], 
		[3 Month] AS [90], 
		[3 month]+([4 month]-[3 month])*1/3 AS [100], 
		[4 Month] AS [120], 
		[5 Month] AS [150], 
		[6 Month] AS [180], 
		([6 month]+[1 year])/2 AS [270], 
		[1 Year] AS [365], 
		([1 year]+[2 year])/2 AS [548], 
		[2 Year] AS [730], 
		([2 Year]+[3 Year])/2 AS [913], 
		[3 Year] AS [1095], 
		[4 Year] AS [1460], 
		[5 Year] AS [1825], 
		[7 Year] AS [2555], 
		[10 Year] AS [3650]
FROM	MRU_Historic_Rates;

GO
/****** Object:  View [dbo].[LogBalanceExclusion]    Script Date: 2/11/2023 1:33:40 pm ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


CREATE VIEW [dbo].[LogBalanceExclusion] AS

/********************************************************************************************************
 * List any balance exclusions from liquidity:
 * - AccountNo = '2345525' in 'S30./1/2/3' (Kiwibank internal account)
 *
 * This view is used for operational control purpose and does not interact with daily regulatory calculation.
 *
 ********************************************************************************************************
 *
 * @version 1.0
 * @author	Stephen Chin
 * @date	2020.09.03
 * 
 * - Start version control
 *
 *
*/

SELECT	EodDate, AccountNo, Product, EodBalance 
FROM	Ultracs.AccountsEOD 
WHERE	AccountNo IN ('2345525') AND (Product = 'S30' OR Product = 'S30.1' OR Product = 'S30.2' OR Product = 'S30.3');

GO
/****** Object:  View [dbo].[LogRetailTermDeposit]    Script Date: 2/11/2023 1:33:40 pm ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE VIEW [dbo].[LogRetailTermDeposit] AS

/********************************************************************************************************
 * List cases where retail term deposit balance is different between TD history and AccountsEOD input tables.
 * Note Liquidity model assumes that EOD balance is correct and a source of truth as it reflects the latest transaction.
 * This view is used for operational control purpose and does not interact with daily regulatory calculation.
 *
 ********************************************************************************************************
 *
 * @version 1.0
 * @author	Stephen Chin
 * @date	2020.08.13
 * 
 * - Start version control
 *
 *
*/

SELECT	(Select CONVERT(DATETIME, Variable, 103) From tblVariables Where No = 1) AS ReportDate,
		TD.Accounts AS AccessNo, 
		TD.Product AS Product, 
		TD.Amount AS TDBalance, 
		EOD.EodBalance AS EODBalance
FROM	(Select Accounts, Product, Sum(Amount) As Amount From Ultracs.AccountsTD_Hist Group By Accounts, Product) TD 
		LEFT JOIN Ultracs.AccountsEOD EOD ON TD.Accounts = EOD.AccountNo AND TD.Product = EOD.Product
WHERE	TD.Amount <> EOD.EodBalance
		OR TD.Amount IS NULL
		OR EOD.EodBalance IS NULL;

GO
/****** Object:  View [dbo].[RBNZFacilityUtilisation]    Script Date: 2/11/2023 1:33:40 pm ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO



CREATE VIEW [dbo].[RBNZFacilityUtilisation]
AS
/********************************************************************************************************
 * A summary of RBNZ facility utilisation for asset encumbrance in the RBNZ reporting template.
 *
 ********************************************************************************************************
 *
 * >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
 * @version 1.0
 * @author	Stephen Chin
 * @date	2021.05.05
 * 
 * - Initial version
 *
 *
*/

SELECT	'FLP' AS Facility, TLR.[Type], TLR.[Adj Mapping Profile], TLR.Instrument, TLR.Currency, TLR.[Settle Status],
		SUM(ABS(CAST(ISNULL(REPLACE(REPLACE(REPLACE(TLR.[NZD Book Value], ')', ''), '(', '-') ,',',''), 0) AS money))) AS BookValue,
        SUM(ABS(CAST(ISNULL(REPLACE(REPLACE(REPLACE(TLR.[NZD Carrying Value], ')', ''), '(', '-') ,',',''), 0) AS money))) AS CarryingValue, 
        SUM(ABS(CAST(ISNULL(REPLACE(REPLACE(REPLACE(TLR.[NZD Face Value], ')', ''), '(', '-') ,',',''), 0) AS money))) AS FaceValue, 
        SUM(ABS(CAST(ISNULL(REPLACE(REPLACE(REPLACE(TLR.[NZD Market Value], ')', ''), '(', '-') ,',',''), 0) AS money))) AS MarketValue, 
		SUM(ABS(CAST(ISNULL(REPLACE(REPLACE(REPLACE(TLR.[NZD Settlement Value], ')', ''), '(', '-') ,',',''), 0) AS money))) AS SettleValue
FROM	MRU_TMS_Liquidity_Reporting TLR
WHERE	[Deal No] IN (Select [Deal No] From MRU_TMS_Liquidity_Reporting Where Instrument Like '%FLP%') AND [Transaction Type] = 'SE'
GROUP BY [Type], [Adj Mapping Profile], Instrument, Currency, [Settle Status]

UNION ALL

SELECT	'TLF' AS Facility, TLR.[Type], TLR.[Adj Mapping Profile], TLR.Instrument, TLR.Currency, TLR.[Settle Status],
		SUM(ABS(CAST(ISNULL(REPLACE(REPLACE(REPLACE(TLR.[NZD Book Value], ')', ''), '(', '-') ,',',''), 0) AS money))) AS BookValue,
        SUM(ABS(CAST(ISNULL(REPLACE(REPLACE(REPLACE(TLR.[NZD Carrying Value], ')', ''), '(', '-') ,',',''), 0) AS money))) AS CarryingValue, 
        SUM(ABS(CAST(ISNULL(REPLACE(REPLACE(REPLACE(TLR.[NZD Face Value], ')', ''), '(', '-') ,',',''), 0) AS money))) AS FaceValue, 
        SUM(ABS(CAST(ISNULL(REPLACE(REPLACE(REPLACE(TLR.[NZD Market Value], ')', ''), '(', '-') ,',',''), 0) AS money))) AS MarketValue, 
		SUM(ABS(CAST(ISNULL(REPLACE(REPLACE(REPLACE(TLR.[NZD Settlement Value], ')', ''), '(', '-') ,',',''), 0) AS money))) AS SettleValue
FROM	MRU_TMS_Liquidity_Reporting TLR
WHERE	[Deal No] IN (Select [Deal No] From MRU_TMS_Liquidity_Reporting Where Instrument Like '%TLF%') AND [Transaction Type] = 'SE'
GROUP BY [Type], [Adj Mapping Profile], Instrument, Currency, [Settle Status]

UNION ALL

SELECT	'TAF' AS Facility, TLR.[Type], TLR.[Adj Mapping Profile], TLR.Instrument, TLR.Currency, TLR.[Settle Status],
		SUM(ABS(CAST(ISNULL(REPLACE(REPLACE(REPLACE(TLR.[NZD Book Value], ')', ''), '(', '-') ,',',''), 0) AS money))) AS BookValue,
        SUM(ABS(CAST(ISNULL(REPLACE(REPLACE(REPLACE(TLR.[NZD Carrying Value], ')', ''), '(', '-') ,',',''), 0) AS money))) AS CarryingValue, 
        SUM(ABS(CAST(ISNULL(REPLACE(REPLACE(REPLACE(TLR.[NZD Face Value], ')', ''), '(', '-') ,',',''), 0) AS money))) AS FaceValue, 
        SUM(ABS(CAST(ISNULL(REPLACE(REPLACE(REPLACE(TLR.[NZD Market Value], ')', ''), '(', '-') ,',',''), 0) AS money))) AS MarketValue, 
		SUM(ABS(CAST(ISNULL(REPLACE(REPLACE(REPLACE(TLR.[NZD Settlement Value], ')', ''), '(', '-') ,',',''), 0) AS money))) AS SettleValue
FROM	MRU_TMS_Liquidity_Reporting TLR
WHERE	[Deal No] IN (Select [Deal No] From MRU_TMS_Liquidity_Reporting Where Instrument Like '%TAF%') AND [Transaction Type] = 'SE'
GROUP BY [Type], [Adj Mapping Profile], Instrument, Currency, [Settle Status]

UNION ALL

SELECT	'OTHER' AS Facility, TLR.[Type], TLR.[Adj Mapping Profile], TLR.Instrument, TLR.Currency, TLR.[Settle Status],
		SUM(ABS(CAST(ISNULL(REPLACE(REPLACE(REPLACE(TLR.[NZD Book Value], ')', ''), '(', '-') ,',',''), 0) AS money))) AS BookValue,
        SUM(ABS(CAST(ISNULL(REPLACE(REPLACE(REPLACE(TLR.[NZD Carrying Value], ')', ''), '(', '-') ,',',''), 0) AS money))) AS CarryingValue, 
        SUM(ABS(CAST(ISNULL(REPLACE(REPLACE(REPLACE(TLR.[NZD Face Value], ')', ''), '(', '-') ,',',''), 0) AS money))) AS FaceValue, 
        SUM(ABS(CAST(ISNULL(REPLACE(REPLACE(REPLACE(TLR.[NZD Market Value], ')', ''), '(', '-') ,',',''), 0) AS money))) AS MarketValue, 
		SUM(ABS(CAST(ISNULL(REPLACE(REPLACE(REPLACE(TLR.[NZD Settlement Value], ')', ''), '(', '-') ,',',''), 0) AS money))) AS SettleValue
FROM	MRU_TMS_Liquidity_Reporting TLR
WHERE	Counterparty = 'Reserve Bank of New Zealand' 
		AND [Deal No] NOT IN (Select [Deal No] From MRU_TMS_Liquidity_Reporting Where Instrument Like '%FLP%')
		AND [Deal No] NOT IN (Select [Deal No] From MRU_TMS_Liquidity_Reporting Where Instrument Like '%TLF%')
		AND [Deal No] NOT IN (Select [Deal No] From MRU_TMS_Liquidity_Reporting Where Instrument Like '%TAF%')
		AND [Transaction Type] = 'SE'
GROUP BY [Type], [Adj Mapping Profile], Instrument, Currency, [Settle Status];


GO
/****** Object:  View [dbo].[RetailTermDepositFull]    Script Date: 2/11/2023 1:33:40 pm ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE VIEW [dbo].[RetailTermDepositFull] 
AS

/* ************************************************************************************************
** Term deposit T&C:                                                                             ** 
**                                                                                               **
** If You choose to invest Your Term Deposit for a term of 2 years or more,                      **
** You can access a portion (up to 20%) of Your initial investment at any time                   **
** without reducing the interest You will receive on that portion.                               **
**                                                                                               **
** Term of 2 years is defined as 730 days.                                                       **
** _____________________________________________________________________________________________ **
**                                                                                               **
** V1.13 Update NewAmount logic to avoid overwriting the total product amount to individual term **
**       deposit amount when one customer having more than one term deposits of one product      **
** Author: Hien Le                                                                               **
** Date: 04/10/2022                                                                              **
** _____________________________________________________________________________________________ **
**                                                                                               **
** @version: 1.3 - model change V1.13.2 - Update New logic for Joint Account Aggregation         **
** @Author : My Phan                                                                             **
** @Date   : 20230523                                                                            **
**                                                                                               **
** Multiple deposits addressed in model change SQL1.13 cause an issue of aggregation of Joint    **
** Account. This is a amended logic to address the issue of SQL1.13 and JA aggregation KB logic  **
** for TD is all customers will have only one TD for 1 product line on 1 day. Therefore, if the  **
** customer has more than 1 TD contract of the same product, on the same day. KB logic will      **
** treat it as double count.                                                                     **
**                                                                                               **
** So this model change logic is to dataset into customers having 1 TD of 1 product on 1 day,    ** 
** and customers having more than 1 TD else equal.                                               **
**  - with customers having more than 1 TD: using logic of v1.13                                 **
**  - with customers having 1 TD: using the original logic (before v1.13)                        **
**                                                                                               **
** Purpose of this view:                                                                         **
**    (1) Compiles TD's Contracted vs. Breakable                                                 **
**                                                                                               **
** The specific issue noted:                                                                     **
**    (2) For customers with more than one TD (where matching on product and maturity),          **
**        - LiquidityDB represents this data as follows,                                         **
**            - In Ultracs.AccountsTD_Hist: One record for each TD                               **
**            - In MRU_RetailTD_Init: One record for each TD                                     **
**	          - In Ultracs.AccountsEOD: One record, and summing all TD Balances (irrespective of **
**              maturity, lodgement)                                                             **
**                                                                                               **
**    (3) The previous version of this view:                                                     **
**            - Joins across these three tables; and,                                            **
**            - Presents the total TD total amount into each of the TD’s,                        **
**            - Thereby overstating the NewAmount                                                **
**            - Instead of using the individual TD’s amounts                                     **
**                                                                                               **
** The change introduced into this view:                                                         **
**    (4) As Ultracs.AccountEOD aggregates by Account and Product, dont join to this table.      **
**    (5) Instead join to either _Hist or _Init which presents individual balances into the view.**
**            - Judging that _Init is the better choice as assuming that it would hold the       **
**              current individual balance rather than a historcal individual balance            **
**                                                                                               **
** _____________________________________________________________________________________________ **
**                                                                                               **
** @version  : 1.4                                                                               **
** @location : S:\dept\Finance\Market Risk and Wholesale Accounting\Model\01_Liquidity\SQL\      **
** 	           1.14.a Joint Accounts\Final SQL - send to IT (v1.14.a)                            **
** @filename : 1.VIEW_RetailTermDepositFull - v1 (of 1.14.a).sql                                 **
** @author   : SP Barnarde                                                                       **
** @date     : 20230621                                                                          **
**                                                                                               **
** Background:                                                                                   **
**                                                                                               **
** Joint account processing 'moves' joint account product balances to the joint account holder   **
** who has the highest asset value, and is done by reassigning that joint account balance        **
** record's AccountNo field. The 'original' account number is stored into the field              **
** AccountNo_org. The link between AccountNo and AccountNo_org is located in MRU_AccountsEOD.    **
**                                                                                               **
** Consequence of this:                                                                          **
**                                                                                               **
** The HIST and INIT tables are populated with the original account number. Therefore any joins  **
** involving these tables would need to join to MRU_AccountsEOD.AccountNo_org.                   **
**                                                                                               **
** Purpose of this version:                                                                      **
**                                                                                               **
** Adjusts the join criteria of the first part of the view as it links to Ultacs.AccountsEOD on  **
** AccountNo, and replaces this with MRU_AccountsEOD.AccountNo_org.                              **
**                                                                                               **
** ************************************************************************************************
*/

-- v1.3 note
--	the orginal logic before v1.13 model change will be applied for customers having 1 TD

SELECT	HIST.Accounts,
		HIST.Product,
		HIST.LodegementDate,
		HIST.MaturityDate,
		HIST.Term,
		HIST.IntFreq,
		HIST.Rate,
		HIST.Amount,
		HIST.TDNo,
		HIST.FileCreateDate,		
		ORIG.Amount AS AmountInit,
		ORIG.Amount * 0.2 AS BreakableCap,
		CASE WHEN ORIG.Amount > HIST.Amount THEN ORIG.Amount - HIST.Amount ELSE 0 END AS BrokenAlready,
		dbo.InlineMin(EOD.EodBalance,
			CASE 
				WHEN (CASE WHEN ORIG.Amount > HIST.Amount THEN ORIG.Amount - HIST.Amount ELSE 0 END) > ORIG.Amount * 0.2 
				THEN 0
				ELSE ORIG.Amount * 0.2 - (CASE WHEN ORIG.Amount > HIST.Amount THEN ORIG.Amount - HIST.Amount ELSE 0 END)
				END
			) AS NewBreakable,
		EOD.EodBalance - dbo.InlineMin(EOD.EodBalance,
			CASE 
				WHEN (CASE WHEN ORIG.Amount > HIST.Amount THEN ORIG.Amount - HIST.Amount ELSE 0 END) > ORIG.Amount * 0.2 
				THEN 0
				ELSE ORIG.Amount * 0.2 - (CASE WHEN ORIG.Amount > HIST.Amount THEN ORIG.Amount - HIST.Amount ELSE 0 END)
				END
			) AS NewAmount,
		
		CASE 
			WHEN DATEDIFF(DAY, ORIG.LodegementDate, ORIG.MaturityDate) >= 730 
			THEN 1
			ELSE 0
			END 
			AS BreakableFlag,
		DATEDIFF(DAY, ORIG.LodegementDate, ORIG.MaturityDate) AS InitTermDays
		
FROM	Ultracs.AccountsTD_Hist HIST 
							
		LEFT JOIN MRU_Retail_TD_Init ORIG 
			ON HIST.Accounts = ORIG.AccessNo 
				AND HIST.TDNo = ORIG.TDNumber 
				AND HIST.LodegementDate = ORIG.LodegementDate 
				AND HIST.MaturityDate = ORIG.MaturityDate 
				AND HIST.Product = ORIG.Product
		

		-- v1.4 adjusts this join to connect the HIST record with the original account number
		-- LEFT JOIN Ultracs.AccountsEOD EOD ON HIST.Accounts = EOD.AccountNo AND HIST.Product = EOD.Product
		LEFT JOIN MRU_AccountsEOD EOD 
				ON HIST.Accounts = EOD.AccountNo_org AND HIST.Product = EOD.Product
		

		-- v1.3 Where	HIST.Accounts in (select Histmp.Accounts from AccountsTD_Hist2 Histmp where Histmp.NoofProd < 2)
		WHERE HIST.Accounts in (select tmp2.Accounts from (SELECT tmp.[Accounts], tmp.Product, count(tmp.Product) as NoofProd FROM [Ultracs].[AccountsTD_Hist] tmp group by tmp.accounts, tmp.Product) tmp2 where tmp2.NoofProd <= 1)
		
		
UNION

-- the logic of v1.13 model change is applied for customers having more than 1 TD

SELECT	HIST.Accounts,
		HIST.Product,
		HIST.LodegementDate,
		HIST.MaturityDate,
		HIST.Term,
		HIST.IntFreq,
		HIST.Rate,
		HIST.Amount,
		HIST.TDNo,
		HIST.FileCreateDate,		
		ORIG.Amount AS AmountInit,
		ORIG.Amount * 0.2 AS BreakableCap,
		CASE WHEN ORIG.Amount > HIST.Amount THEN ORIG.Amount - HIST.Amount ELSE 0 END AS BrokenAlready,
		dbo.InlineMin(HIST.Amount,
			CASE 
				WHEN (CASE WHEN ORIG.Amount > HIST.Amount THEN ORIG.Amount - HIST.Amount ELSE 0 END) > ORIG.Amount * 0.2 THEN 0
				ELSE ORIG.Amount * 0.2 - (CASE WHEN ORIG.Amount > HIST.Amount THEN ORIG.Amount - HIST.Amount ELSE 0 END)
				END
			) AS NewBreakable,
				
		HIST.Amount - dbo.InlineMin(HIST.Amount,
			CASE 
				WHEN (CASE WHEN ORIG.Amount > HIST.Amount THEN ORIG.Amount - HIST.Amount ELSE 0 END) > ORIG.Amount * 0.2 THEN 0
				ELSE ORIG.Amount * 0.2 - (CASE WHEN ORIG.Amount > HIST.Amount THEN ORIG.Amount - HIST.Amount ELSE 0 END)
				END
			) AS NewAmount,
		CASE 
			WHEN DATEDIFF(DAY, ORIG.LodegementDate, ORIG.MaturityDate) >= 730 THEN	1 
			ELSE 0
			END AS BreakableFlag,
		DATEDIFF(DAY, ORIG.LodegementDate, ORIG.MaturityDate) AS InitTermDays
		
FROM	Ultracs.AccountsTD_Hist HIST 
					
		LEFT JOIN MRU_Retail_TD_Init ORIG ON HIST.Accounts = ORIG.AccessNo AND HIST.TDNo = ORIG.TDNumber AND HIST.LodegementDate = ORIG.LodegementDate AND HIST.MaturityDate = ORIG.MaturityDate AND HIST.Product = ORIG.Product
			
			-- v1.3 Where	HIST.Accounts in (select Histmp.Accounts from AccountsTD_Hist2 Histmp where Histmp.NoofProd >1)
WHERE	HIST.Accounts in (select tmp2.Accounts from (SELECT tmp.[Accounts], tmp.Product, count(tmp.Product) as NoofProd FROM [Ultracs].[AccountsTD_Hist] tmp group by tmp.accounts, tmp.Product) tmp2 where tmp2.NoofProd >1)
;

GO
/****** Object:  View [dbo].[TMSLiquidityCashflowsFormatted]    Script Date: 2/11/2023 1:33:40 pm ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


CREATE VIEW [dbo].[TMSLiquidityCashflowsFormatted] AS

/********************************************************************************************************
 * This view stores the MRU_TMS_Liquidity_Cashflows in correct data format.
 *
 ********************************************************************************************************
 *
 * @version 1.0
 * @author	Stephen Chin
 * @date	2021.01.14
 * 
 * - Initial version
 * 
 *
*/

SELECT	CAST([Deal Id] as int) as DealId,
		CAST([D1Deal No] as int) as DealNo,
		CAST([Deal Side] as int) as DealSide,
		[Transaction Type], Instrument, Entity, Counterparty, [D1Issuer Name], Currency, 
		CAST([Cash Flow Date] as date) as CashFlowDate,
		[Cash Flow Type], [Type], [Sector], [Mapping Profile], [Adj Mapping Profile], [P1Time Profile], [Adj Time Profile],
		CAST([P1Position Date] as date) as PositionDate,
		[State], RepoFlag, [Settle Status],
		CAST([Dealt Date] as date) as DealtDate,
		CAST([Begin Date] as date) as BeginDate,
		CAST([End Date] as date) as EndDate,
		CAST(REPLACE(REPLACE(REPLACE([Days To Maturity], '(', '-'), ')', ''), ',', '') as money) as DaysToMaturity,
		CAST(REPLACE(REPLACE(REPLACE([Days To Repricing], '(', '-'), ')', ''), ',', '') as money) as DaysToRepricing,
		CAST([P1Repricing Date] as date) as RepricingDate,
		CAST(REPLACE(REPLACE(REPLACE([P1Face Value], '(', '-'), ')', ''), ',', '') as money) as FaceValue,
		CAST(REPLACE(REPLACE(REPLACE([NZD Face Value], '(', '-'), ')', ''), ',', '') as money) as NZDFaceValue,
		CAST(REPLACE(REPLACE(REPLACE([P1Market Value], '(', '-'), ')', ''), ',', '') as money) as MarketValue,
		CAST(REPLACE(REPLACE(REPLACE([NZD Market Value], '(', '-'), ')', ''), ',', '') as money) as NZDMarketValue,
		[P1Bank Account No],
		CAST(REPLACE(REPLACE(REPLACE([P1Bank Balance], '(', '-'), ')', ''), ',', '') as money) as BankBalance,
		CAST(REPLACE(REPLACE(REPLACE([Principal Cashflow], '(', '-'), ')', ''), ',', '') as money) as PrincipalCashflow,
		CAST(REPLACE(REPLACE(REPLACE([Interest Cashflow], '(', '-'), ')', ''), ',', '') as money) as InterestCashflow,
		CAST(REPLACE(REPLACE(REPLACE([Other Cashflow], '(', '-'), ')', ''), ',', '') as money) as OtherCashflow,
		[Flow Type],
		CAST(REPLACE(REPLACE(REPLACE([Total Cashflow], '(', '-'), ')', ''), ',', '') as money) as TotalCashflow,
		[D1Counterparty Name],
		CAST(REPLACE(REPLACE(REPLACE([NZD Principal Cashflow], '(', '-'), ')', ''), ',', '') as money) as NZDPrincipalCashflow,
		CAST(REPLACE(REPLACE(REPLACE([NZD Interest Cashflow], '(', '-'), ')', ''), ',', '') as money) as NZDInterestCashflow,
		CAST(REPLACE(REPLACE(REPLACE([NZD Other Cashflow], '(', '-'), ')', ''), ',', '') as money) as NZDOtherCashflow,
		CAST(REPLACE(REPLACE(REPLACE([NZD Cashflow], '(', '-'), ')', ''), ',', '') as money) as NZDCashflow
FROM	MRU_TMS_Liquidity_Cashflows
WHERE	[Cash Flow Date] <> '1899-12-30';

GO
/****** Object:  View [dbo].[TMSLiquidityReportingFormatted]    Script Date: 2/11/2023 1:33:40 pm ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO



CREATE VIEW [dbo].[TMSLiquidityReportingFormatted] AS

/********************************************************************************************************
 * This view stores the MRU_TMS_Liquidity_Reporting in correct data format.
 *
 ********************************************************************************************************
 *
 * @version 1.0
 * @author	Stephen Chin
 * @date	2021.01.14
 * 
 * @version 2.0 - adding accrued interest variables: NZDAccruedInterest, NZDCleanValue
 * @author  My Phan
 * @date    2022.07.29
 *
 * - Initial version
 * 
 *
*/

SELECT	CAST([Deal Id] as int) as DealId,
		CAST([Deal No] as int) as DealNo,
		CAST([Deal Side] as int) as DealSide,
		[Transaction Type], Instrument, Entity, Counterparty, [Issuer Name], [CptyIssuer Rating], Currency, [Type], Sector, [Adj Mapping Profile], [Time Profile], [Adj Time Profile],
		CAST([Position Date] as date) as PositionDate,
		RepoFlag, [Settle Status],
		CAST([Dealt Date] as date) as DealtDate,
		CAST([Begin Date] as date) as BeginDate,
		CAST([End Date] as date) as EndDate,
		CAST([Days To Maturity] as int) as DaysToMaturity,
		CAST([Days To Repricing] as int) as DaysToRepricing,
		CAST([Repricing Date] as date) as RepricingDate,
		CAST([Spot Factor] as float) as SpotFactor,
		CAST(REPLACE(REPLACE(REPLACE([Face Value], '(', '-'), ')', ''), ',', '') as money) as FaceValue,
		CAST(REPLACE(REPLACE(REPLACE([NZD Face Value], '(', '-'), ')', ''), ',', '') as money) as NZDFaceValue,
		CAST(REPLACE(REPLACE(REPLACE([Market Value], '(', '-'), ')', ''), ',', '') as money) as MarketValue,
		CAST(REPLACE(REPLACE(REPLACE([NZD Market Value], '(', '-'), ')', ''), ',', '') as money) as NZDMarketValue,
		[Bank Account Number],
		CAST(REPLACE(REPLACE(REPLACE([Bank Balance], '(', '-'), ')', ''), ',', '') as money) as BankBalance,
		CAST(REPLACE(REPLACE(REPLACE([NZD Bank Balance], '(', '-'), ')', ''), ',', '') as money) as NZDBankBalance,
		CAST(REPLACE(REPLACE(REPLACE([Book Value], '(', '-'), ')', ''), ',', '') as money) as BookValue,
		CAST(REPLACE(REPLACE(REPLACE([NZD Book Value], '(', '-'), ')', ''), ',', '') as money) as NZDBookValue,
		CAST(REPLACE(REPLACE(REPLACE([Carrying Value], '(', '-'), ')', ''), ',', '') as money) as CarryingValue,
		CAST(REPLACE(REPLACE(REPLACE([NZD Carrying Value], '(', '-'), ')', ''), ',', '') as money) as NZDCarryingValue,
		CAST(REPLACE(REPLACE(REPLACE([Settlement Value], '(', '-'), ')', ''), ',', '') as money) as SettlementValue,
		CAST(REPLACE(REPLACE(REPLACE([NZD Settlement Value], '(', '-'), ')', ''), ',', '') as money) as NZDSettlementValue,
		RepoTypeFlag,
		CAST([Dealt Rate] as float) as DealtRate,
		CAST(REPLACE(REPLACE(REPLACE([NZD Accrued Interest], '(', '-'), ')', ''), ',', '') as money) as NZDAccruedInterest,
		CAST(REPLACE(REPLACE(REPLACE([NZD Clean Value], '(', '-'), ')', ''), ',', '') as money) as NZDCleanValue
FROM	MRU_TMS_Liquidity_Reporting
WHERE	[Position Date] <> '1899-12-30';

GO
/****** Object:  View [dbo].[TMSLiquids]    Script Date: 2/11/2023 1:33:40 pm ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
	
CREATE VIEW [dbo].[TMSLiquids]
AS

/********************************************************************************************************
 * This is another view of TMS Liquidity Report (Analytics query output) with being haircut applied.
 * Note that it is miscellaneous and not being used in regulatory calculation.
 *
 ********************************************************************************************************
 *
 * @version 1.0
 * @author	Stephen Chin
 * @date	2020.08.13
 * 
 * - Start version control
 * - Exclude data where position date = '1899-12-30'
 *
*/

SELECT		TMS.[Position Date] AS EODDate, 
			TMS.[Type], 
			CASE WHEN TMS.Sector = 'GovtIIBond' THEN 'GovtBond' ELSE TMS.Sector END Sector, 
			TMS.[CptyIssuer Rating] AS Rating, 
			TMS.[Settle Status], 
			CASE WHEN TMS.RepoFlag = 'N' THEN
				TMS.RepoFlag
			ELSE
				CASE WHEN TMS.RepoTypeFlag = 'R' THEN
					TMS.RepoTypeFlag
				ELSE
					TMS.RepoFlag
				END
			END AS RepoType, 
			TMS.[Deal Id], 
			TMS.[Deal No], 
			TMS.[Transaction Type], 
			TMS.Instrument, 
			TMS.Entity, 
			TMS.Counterparty, 
			TMS.[Issuer Name], 
			CASE WHEN TMS.Sector LIKE 'Cash%' THEN TMS.[Position Date] ELSE ISNULL(TMS.[Begin Date], TMS.[Position Date]) END AS BeginDate,
			CASE WHEN TMS.Sector LIKE 'Cash%' THEN DATEADD(DD, 1, TMS.[Position Date]) ELSE ISNULL(TMS.[End Date], DATEADD(DD, 1, TMS.[Position Date])) END AS EndDate,
			DATEDIFF(DD, TMS.[Position Date], ISNULL(CASE WHEN TMS.Sector LIKE 'Cash%' THEN TMS.[Position Date] ELSE ISNULL(TMS.[Begin Date], TMS.[Position Date]) END, DATEADD(DD, 1, TMS.[Position Date]))) AS DaysToStart,
			CASE WHEN DATEDIFF(DD, TMS.[Position Date], ISNULL(CASE WHEN TMS.Sector LIKE 'Cash%' THEN TMS.[Position Date] ELSE ISNULL(TMS.[Begin Date], TMS.[Position Date]) END, DATEADD(DD, 1, TMS.[Position Date]))) > 0 THEN 
				dbo.GetBucketProfileFunding(dbo.GetTimeProfile(TMS.[Position Date], DATEDIFF(d, TMS.[Position Date], ISNULL(CASE WHEN Sector LIKE 'Cash%' THEN TMS.[Position Date] ELSE ISNULL([Begin Date], TMS.[Position Date]) END, DATEADD(d,1,TMS.[Position Date])))))
			ELSE 
				'Started' 
			END AS StartBucket, 
			DATEDIFF(DD, TMS.[Position Date], CASE WHEN TMS.Sector LIKE 'Cash%' THEN DATEADD(DD, 1, TMS.[Position Date]) ELSE ISNULL(TMS.[End Date], DATEADD(DD, 1, TMS.[Position Date])) END) AS DaysToMaturity,
			dbo.GetBucketProfileFunding(dbo.GetTimeProfile(TMS.[Position Date], DATEDIFF(DD, TMS.[Position Date], CASE WHEN TMS.Sector LIKE 'Cash%' THEN DATEADD(DD, 1, TMS.[Position Date]) ELSE ISNULL(TMS.[End Date], DATEADD(DD, 1, TMS.[Position Date])) END))) AS BucketProfileCashflow,
			CASE WHEN TMS.[NZD Face Value] <> '' THEN CAST(REPLACE(REPLACE(REPLACE(TMS.[NZD Face Value],'(', '-'),')',''), ',','') AS DECIMAL(18,2)) ELSE 0 END AS NZDFaceValue,
			TMS.[NZD Face Value] AS NZDFaceValue_Bracketed,
			CASE WHEN TMS.[NZD Market Value] <> '' THEN  CAST(REPLACE(REPLACE(REPLACE(TMS.[NZD Market Value],'(', '-'),')',''),',','') AS DECIMAL(18,2)) ELSE 0 END AS NZDMarketValue, 
			TMS.[NZD Market Value] AS NZDMarketValue_Bracketed,
			RCF.Tier1Haircut, 
			RCF.Tier2Haircut, 
			
			CASE WHEN DATEDIFF(DD, TMS.[Position Date], DATEADD(YY, 3, TMS.[Position Date])) > DATEDIFF(DD, TMS.[Position Date], CASE WHEN TMS.Sector LIKE 'Cash%' THEN DATEADD(DD, 1, TMS.[Position Date]) ELSE ISNULL(TMS.[End Date], DATEADD(DD, 1, TMS.[Position Date])) END) THEN 
				'Tier1'
			ELSE
				'Tier2'
			END AS HaircutBucket,
			
			CASE WHEN DATEDIFF(DD, TMS.[Position Date], DATEADD(YY, 3, TMS.[Position Date])) > DATEDIFF(DD, TMS.[Position Date], CASE WHEN TMS.Sector LIKE 'Cash%' THEN DATEADD(DD, 1, TMS.[Position Date]) ELSE ISNULL(TMS.[End Date], DATEADD(DD, 1, TMS.[Position Date])) END) THEN 
				CASE WHEN RCF.Tier1Haircut = 'NA' THEN
					'100.0'
				ELSE
					RCF.Tier1Haircut
				END
			ELSE
				CASE WHEN RCF.Tier2Haircut = 'NA' THEN
					'100.0'
				ELSE
					RCF.Tier2Haircut
				END
			END AS HaircutFactor, 
			
			CASE WHEN TMS.[NZD Market Value] <> '' THEN  CAST(REPLACE(REPLACE(REPLACE(TMS.[NZD Market Value], '(', '-'), ')', ''), ',', '') AS DECIMAL(18,2)) ELSE 0 END 
				* (1 - (CASE WHEN DATEDIFF(DD, TMS.[Position Date], DATEADD(YY, 3, TMS.[Position Date])) > DATEDIFF(DD, TMS.[Position Date], CASE WHEN TMS.[Sector] LIKE 'Cash%' THEN DATEADD(DD, 1, TMS.[Position Date]) ELSE ISNULL(TMS.[End Date], DATEADD(DD, 1, TMS.[Position Date])) END) THEN
				
				CASE WHEN RCF.Tier1Haircut = 'NA' THEN
					100.0
				ELSE
					CAST(RCF.Tier1Haircut AS FLOAT)
				END
			ELSE
				CASE WHEN RCF.Tier2Haircut = 'NA' THEN
					100.0
				ELSE
					CAST(RCF.Tier2Haircut AS FLOAT)
				END
			END / 100.0)) AS NZDHaircutMarketValue,
			
			CASE WHEN TMS.[NZD Settlement Value] <> '' THEN CAST(REPLACE(REPLACE(REPLACE(TMS.[NZD Settlement Value], '(', '-'), ')', ''), ',', '') AS FLOAT) ELSE 0 END AS NZDSettlementValue,
			
			TMS.RepoFlag

FROM		MRU_TMS_Liquidity_Reporting TMS
			LEFT JOIN MRU_RBNZ_CoverFactors RCF ON TMS.[CptyIssuer Rating] = RCF.Rating AND TMS.Sector = RCF.Class
			
WHERE		TMS.[Position Date] <> '1899-12-30'

GO
/****** Object:  View [dbo].[TMSPledgedAssets]    Script Date: 2/11/2023 1:33:40 pm ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE VIEW [dbo].[TMSPledgedAssets]
AS
/********************************************************************************************************
 * This stores security leg of TLF or FLP deals where collateral is RMBS.
 *
 ********************************************************************************************************
 *
 * >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
 * @version 1.0
 * @author	Stephen Chin
 * @date	2021.01.15
 * 
 * - Initial version
 *
 * >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
 * @version 1.1
 * @author	Stephen Chin
 * @date	2021.04.12
 *
 * - Extend old view (TMSRMBSPledgedAssets) to all pledged assets, and rename accordingly
 *
*/

SELECT	[Deal Id], [Deal No], [Type], [Adj Mapping Profile], Instrument, Currency, [Settle Status], [Begin Date], [End Date], 
		ABS(CAST(ISNULL(REPLACE(REPLACE(REPLACE([NZD Book Value], ')', ''), '(', '-') ,',',''), 0) AS DECIMAL(18,2))) AS BookValue,
        ABS(CAST(ISNULL(REPLACE(REPLACE(REPLACE([NZD Carrying Value], ')', ''), '(', '-') ,',',''), 0) AS DECIMAL(18,2))) AS CarryingValue, 
        ABS(CAST(ISNULL(REPLACE(REPLACE(REPLACE([NZD Face Value], ')', ''), '(', '-') ,',',''), 0) AS DECIMAL(18,2))) AS FaceValue, 
        ABS(CAST(ISNULL(REPLACE(REPLACE(REPLACE([NZD Market Value], ')', ''), '(', '-') ,',',''), 0) AS DECIMAL(18,2))) AS MarketValue, 
		ABS(CAST(ISNULL(REPLACE(REPLACE(REPLACE([NZD Settlement Value], ')', ''), '(', '-') ,',',''), 0) AS DECIMAL(18,2))) AS SettleValue 
FROM	MRU_TMS_Liquidity_Reporting
WHERE	[Type] IN ('Prime Liquidity', 'Secondary Liquidity') 
		AND RepoFlag = 'Y' 
		AND RepoTypeFlag = 'C';

GO
/****** Object:  StoredProcedure [CORP\trebatadm01].[CompareTables]    Script Date: 2/11/2023 1:33:40 pm ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [CORP\trebatadm01].[CompareTables](@table1 varchar(100), 
 @table2 Varchar(100), @T1ColumnList varchar(1000),
 @T2ColumnList varchar(1000) = '')
AS
 
-- Table1, Table2 are the tables or views to compare.
-- T1ColumnList is the list of columns to compare, from table1.
-- Just list them comma-separated, like in a GROUP BY clause.
-- If T2ColumnList is not specified, it is assumed to be the same
-- as T1ColumnList.  Otherwise, list the columns of Table2 in
-- the same order as the columns in table1 that you wish to compare.
--
-- The result is all rows from either table that do NOT match
-- the other table in all columns specified, along with which table that
-- row is from.
 
declare @SQL varchar(8000);
 
IF @t2ColumnList = '' SET @T2ColumnList = @T1ColumnList
 
set @SQL = 'SELECT ''' + @table1 + ''' AS TableName, ' + @t1ColumnList +
 ' FROM ' + @Table1 + ' UNION ALL SELECT ''' + @table2 + ''' As TableName, ' +
 @t2ColumnList + ' FROM ' + @Table2
 
set @SQL = 'SELECT Max(TableName) as TableName, ' + @t1ColumnList +
 ' FROM (' + @SQL + ') A GROUP BY ' + @t1ColumnList + 
 ' HAVING COUNT(*) = 1'
 
exec ( @SQL)
GO
/****** Object:  StoredProcedure [dbo].[check_counts_Liquidity]    Script Date: 2/11/2023 1:33:40 pm ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE procedure [dbo].[check_counts_Liquidity]  @status int output as

	Set @status = (select case 
	when (select count(*) from Ultracs.AccountsEOD) > 0 and (select count(*) from Ultracs.AccountStaticAMP) > 0
	and (select count(*) from Ultracs.AccountStaticKB) > 0 and (select count(*) from Ultracs.AccountStaticNZHL) > 0
	and (select count(*) from Ultracs.AccountsTD_Hist) > 0
	and (select count(*) from Ultracs.CustomersAMP) > 0 
	and (select count(*) from Ultracs.CustomersBusinessLinks) > 0
	and (select count(*) from Ultracs.CustomersKB ) > 0 
	and (select count(*) from MRU_Retail_Loan_Cashflows ) > 0 
	and (select count(*) from MRU_Retail_TD_Cashflows ) > 0 
	and (select count(*) from Ultracs.CustomersNZHL) >  0 then 1 Else 0 End) 
	--and (select count(*) from ActivateKB.vwActivateApplicationsSummary ) > 0 
	--and (select count(*) from ActivateKB.Activate_Loan_Components ) > 0 
	--and (select count(*) from ActivateKB.A_Activate_Applications ) > 0 
	--and (select count(*) from ActivateBB.Activate_Loan_Portions ) > 0
	--and (select count(*) from ActivateBB.Activate_Applications ) > 0 


	
GO
/****** Object:  StoredProcedure [dbo].[FileImport_Insert]    Script Date: 2/11/2023 1:33:40 pm ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[FileImport_Insert]
	
	@FilePath VARCHAR(255),
	@ModifiedDate DATETIME,
	@ImportedDate DATETIME
	
AS
BEGIN
	
	SET NOCOUNT ON;

    INSERT INTO [FileImport]
           ([FilePath]
           ,[ModifiedDate]
           ,[ImportedDate])
     VALUES
           (@FilePath, 
           @ModifiedDate, 
           @ImportedDate);
END
GO
/****** Object:  StoredProcedure [dbo].[FileImport_SelectCount]    Script Date: 2/11/2023 1:33:40 pm ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [dbo].[FileImport_SelectCount]
	@FilePath VARCHAR(255), @ModifiedDate DATETIME
AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

    Select COUNT(*) AS FileImportCount
	FROM FileImport
	WHERE FilePath = @FilePath
	AND ModifiedDate = @ModifiedDate
END
GO
/****** Object:  StoredProcedure [dbo].[GetBusinessDayOfMonth]    Script Date: 2/11/2023 1:33:40 pm ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[GetBusinessDayOfMonth]	
AS
BEGIN	
	SET NOCOUNT ON;
	
	
	DECLARE @ToDate DATE = CAST(getdate() AS DATE);
	DECLARE @FromDate DATE = DATEADD(month, DATEDIFF(month, 0, @ToDate), 0);
	
    
	SELECT Calendar.dbo.BusinessDaysBetween(@FromDate, @ToDate, 'NZ') as BusinessDayOfMonth;
END
GO
/****** Object:  StoredProcedure [dbo].[GetPreviousBusinessDay]    Script Date: 2/11/2023 1:33:40 pm ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


CREATE PROCEDURE [dbo].[GetPreviousBusinessDay]
	@ValueDate DATE, 
	@CountryCode VARCHAR(10)
AS
BEGIN
	 
	SET NOCOUNT ON;

    SELECT CAST(Calendar.dbo.AdvanceBusinessDays(@ValueDate, -1, @CountryCode) AS DATETIME) AS PreviousBusinessDay
END

GO
/****** Object:  StoredProcedure [dbo].[MRU_AccountsBalanceJoint_MakeTable]    Script Date: 2/11/2023 1:33:40 pm ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO




CREATE PROCEDURE [dbo].[MRU_AccountsBalanceJoint_MakeTable]	
AS
BEGIN
	
SET NOCOUNT ON;

/********************************************************************************************************
 * The procedure makes table MRU_AccountsBalanceJoint as an interim table to update account numbers for Joint Accounts
 * i.e., Comparing indivual balances of joint accounts, ptotal = total balances of primary account holders, stotal =  total balances of secondary account holders
 *
 ********************************************************************************************************
 *
 * @version 1.0 - Model Change SQL 1.14 - Joint Account
 * @author	My Phan
 * @date	2023.05.08
 * 
 * - Initial version
 *
 *
*/

/* Establish a temporary table consisting of account individual balances. This is determined by joining 
** AccountsEOD data (table Ultracs.AccountsEOD_Original) with Ultracs.UltracsRelationships.
**
** Individual accounts are deemed to have relationship type classified as any one of 'BTR','BUS','IND','ITR'
**
** As regulation requires total assets, only positive balances are considered, and also, retail credit cards
** and similar products are excluded (BS13 48(a)). 
**
** To ensure that total assets are aggreated across relationship types, group only by AccountNo. Including 
** RelType in the group statement will result in instances of multiple individual account balances; and would 
** introduce type_of_duplicates inot the dataset. 
**
*/
DECLARE	@AccountTotBal AS TABLE (AccountNo nvarchar(10), TotalBalance money, RelType nvarchar(10))
INSERT	@AccountTotBal (AccountNo, TotalBalance, RelType)
SELECT	EOD.AccountNo, SUM(EodBalance), 'IND'
FROM	Ultracs.AccountsEOD_Original EOD
		LEFT JOIN Ultracs.UltracsRelationships UR ON EOD.AccountNo = UR.AccountNo AND EOD.Product = UR.Product 
WHERE	UR.AccountNo = UR.CustomerNo 
		AND UR.RelType IN ('BTR','BUS','IND','ITR')   -- viz. Individual Account Identifier
		AND EOD.EodBalance >= 0
		AND EOD.Product NOT LIKE 'S7%' AND EOD.Product NOT LIKE 'S80%' AND EOD.Product NOT LIKE 'S81%'
		AND EOD.Product NOT LIKE 'S82%' AND EOD.Product NOT LIKE 'S83%' AND EOD.Product NOT LIKE 'S84%'
		AND EOD.Product NOT LIKE 'S85%' AND EOD.Product NOT LIKE 'S86%' AND EOD.Product NOT LIKE 'S87%' 
		AND EOD.Product NOT LIKE 'S88%' AND EOD.Product NOT LIKE 'S89%' AND EOD.Product NOT LIKE 'S91%' 
		AND EOD.Product NOT LIKE 'S92%' AND EOD.Product NOT LIKE 'S93%' AND EOD.Product NOT LIKE 'S94%' 
		AND EOD.Product NOT LIKE 'S95%' AND EOD.Product NOT LIKE 'S96%' AND EOD.Product NOT LIKE 'S97%' 
		AND EOD.Product NOT LIKE 'S98%' AND EOD.Product NOT LIKE 'S99%'
GROUP BY EOD.AccountNo;

/* Establish MRU_AccountsBalanceJoint consisting of any type of secondary RelType classification. These are
** SJT Secondary Joint Owner, and BSJT  Business Secondary Joint Owner.
**
** Associate with each joint account (accountNo, Customer), the product and relationship type, as well as the 
** individual account balance for the Account and the Customer. 
**
** As this is a left join from Ultracs.UltracsRelationships  to the temporary table, accounts for which the 
** pTotal or sTotal do not exist are presented as NULLs within the datset. 
**
*/
DELETE FROM MRU_AccountsBalanceJoint;
INSERT	MRU_AccountsBalanceJoint
SELECT	UR.AccountNo, UR.CustomerNo, UR.Product, UR.RelType,
		TBP.TotalBalance as pTotal,
		TBS.TotalBalance as sTotal
FROM	Ultracs.UltracsRelationships UR
		LEFT JOIN @AccountTotBal TBP on UR.AccountNo = TBP.AccountNo
		LEFT JOIN @AccountTotBal TBS on UR.CustomerNo = TBS.AccountNo
WHERE	UR.RelType LIKE '%SJT';



END


GO
/****** Object:  StoredProcedure [dbo].[MRU_AccountsEOD_Update]    Script Date: 2/11/2023 1:33:40 pm ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO




CREATE PROCEDURE [dbo].[MRU_AccountsEOD_Update]
AS
BEGIN
	
SET NOCOUNT ON;

/********************************************************************************************************
 * The procedure updates Ultracs.AccountsEOD table
 * i.e., Override account numbers of Joint accounts
 * Note this is only to update account numbers 
 *
 ********************************************************************************************************
 *
 * @version 1.0 - Model Change SQL 1.14 - Joint Account
 * @author	My Phan
 * @date	2023.05.08
 * 
 * - Initial version
 *
 *
*/

/* Establish MRU_AccountsEOD by inserting all data from Ultracs.AccountsEOD_Original. Secure the original 
** account number as it is overwritten by the UPDATE statement that follows; and is needed so that the 
** reassigned EOD balances remain able to join to UltracsRelationships; and helps identify where balances 
** have been reassigned.
*/
DELETE FROM	MRU_AccountsEOD
INSERT INTO		MRU_AccountsEOD
SELECT			EODO.AccountNo as AccountNo,
				EODO.AccountNo as AccountNo_Org,
				EODO.Product as Product,
				EODO.EodDate as EoDDate,
				EODO.EODLimit as EODLimit,
				EODO.EodBalance as EodBalance

FROM			Ultracs.AccountsEOD_Original EODO;

/* For Accounts where the primary account holder has a individual balance less than that of the CustomerNo
** individual balance, replace the Account Number with that CustomerNo. 
**
** The CustomerNo that replaces the Account is determined by identifying the CustomerNo that has the largest 
** individual balance, when grouped by Account and Product. 
**
** As more than one customer in this grouping may have the same individual balance as other customers in this 
** grouping, return only one of these customers. This is needed so that the subquery determining Customer joins 
** only one record to the MRU_AccountsEOD sql action.
**
** As ANSI NULLS is on, need to accomodate for instances where pTotal or sTotal is NULL, therefore wrap in isnull 
**
*/
UPDATE		MRU_AccountsEOD
SET			AccountNo = customerno

	FROM	(  SELECT	Account, CustomerNo, Product, RelType, pTotal, sTotal
               FROM		MRU_AccountsBalanceJoint ABJ
               WHERE  ( ((isnull(pTotal,0) < isnull(sTotal,0)) OR (pTotal is null and sTotal is not null) ) ) 
			   AND CustomerNo =  (SELECT    TOP 1
											CustomerNo
                                    FROM    MRU_AccountsBalanceJoint ABJ 
                                                  ,(SELECT       TOP 1
                                                                Account, Product, pTotal, MAX (sTotal) AS max_sTotal
                                                         FROM       MRU_AccountsBalanceJoint ABJ2
                                                         WHERE       ABJ2.Account = abj.account
                                                         AND abj2.product = abj.product
                                                         GROUP BY Account, Product, pTotal
                                                  )  ABJ3
                                    WHERE  ABJ.Account = ABJ3.Account
                                           AND ABJ.Product = ABJ3.Product
                                            AND ABJ.sTotal = ABJ3.max_sTotal
                             )
                      ) TMP


	WHERE	MRU_AccountsEOD.AccountNo = tmp.Account 
			AND MRU_AccountsEOD.Product = tmp.Product

/** Move the data from this working table to the Ultracs.AccountsEOD table. Do this by replacing all of the data 
** in Ultracs.AccountsEOD; and prevents duplicate records.
**
** Noting that Ultracs.AccountsEOD is sourced external from LiquidityDB. The original state of Ultracs.AccountsEOD
** is established to Ultracs.AccountsEOD_Original, and done as part of the IT data refresh processes. 
**
** It would be preferred to use a table different from Ultracs.AccountsEOD to record the updated (reassigned) 
** AccountsEOD data, and to have Ultracs.AccountsEOD rather represent the original data state. However the approach 
** chosen ensures that any other existing usecases consuming from Ultracs.AccountsEOD can remain unchanged and 
** thereby would by defauly consume the updated AccountEOD data.
**
*/
DELETE FROM	Ultracs.AccountsEOD
INSERT INTO		Ultracs.AccountsEOD
SELECT			EODM.AccountNo as AccountNo,
				EODM.Product as Product,
				EODM.EodDate as EoDDate,
				EODM.EODLimit as EODLimit,
				EODM.EodBalance as EodBalance

FROM MRU_AccountsEOD EODM;


END

GO
/****** Object:  StoredProcedure [dbo].[MRU_Client_Bandings_MakeTable]    Script Date: 2/11/2023 1:33:40 pm ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[MRU_Client_Bandings_MakeTable]
AS
BEGIN	
SET NOCOUNT ON;

/********************************************************************************************************
 * This assigns size band depending on the client total balance according to BS13. 
 *
 ********************************************************************************************************
 *
 * @version 1.0
 * @author	Stephen Chin
 * @date	2020.08.13
 * 
 * - Start version control
 *
*/

TRUNCATE TABLE MRU_Client_Bandings;

INSERT		MRU_Client_Bandings
			(GroupAccountNo, Balance, Banding)
	
SELECT		GroupAccountNo AS GroupAccountNo,  
			SUM(EOD_Balance) AS Balance, 
			dbo.GetSizeBand(SUM(EOD_Balance)) AS Banding  
FROM		ClientBalance
GROUP BY	GroupAccountNo, EodDate;

END

GO
/****** Object:  StoredProcedure [dbo].[MRU_FO_Funding_Balances_MakeTable]    Script Date: 2/11/2023 1:33:40 pm ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[MRU_FO_Funding_Balances_MakeTable]
AS
BEGIN
	
SET NOCOUNT ON;

/********************************************************************************************************
 * This populates a miscellaneous table that stores funding detail and not used in regulatory calculation.
 *
 ********************************************************************************************************
 *
 * @version 1.0
 * @author	Stephen Chin
 * @date	2020.08.13
 * 
 * - Start version control
 *
 *
*/

Declare	@reportDate date
Set		@reportDate = (Select CONVERT(DATE, Variable, 103) From tblVariables Where NO = 1);

TRUNCATE TABLE MRU_FO_Funding_Balances;

INSERT		MRU_FO_Funding_Balances

SELECT		AD.FundingType, 
			CASE WHEN ProductGroup = 'MTNFLT' THEN
				'MTN'
			ELSE
				CASE WHEN ProductGroup = 'SENFLT' THEN
					'SEN'
				ELSE
					CASE WHEN ProductGroup = 'SUBFLT' THEN
						'SUB'
					ELSE
						ProductGroup
					END
				END
			END AS ProductGrouping, 
			CASE WHEN ProductType <> 'Term' THEN 'Transactional' ELSE ProductType END AS AccountType, 
			CASE WHEN Sector <> 'Wholesale' THEN 'Retail' ELSE Sector END AS Business, 
			AD.Banding,
			dbo.GetBucketProfileFunding(dbo.GetTimeProfile(@reportDate, RemaingTermDays)) AS BucketProfileCashflow,
			SUM(AD.Balance) AS [Funding Balance], 
			AD.EodDate 
	
FROM		AllData AD
	
GROUP BY	AD.FundingType,
			CASE WHEN ProductGroup = 'MTNFLT' THEN
				'MTN'
			ELSE
				CASE WHEN ProductGroup = 'SENFLT' THEN
					'SEN'
				ELSE
					CASE WHEN ProductGroup = 'SUBFLT' THEN
						'SUB'
					ELSE
						ProductGroup
					END
				END
			END, 
			CASE WHEN ProductType <> 'Term' THEN 'Transactional' ELSE ProductType END, 
			CASE WHEN Sector <> 'Wholesale' THEN 'Retail' ELSE Sector END, 
			AD.Banding, 
			dbo.GetBucketProfileFunding(dbo.GetTimeProfile(@reportDate, RemaingTermDays)), 
			AD.EodDate
	
ORDER BY	AD.FundingType, 
			CASE WHEN ProductGroup = 'MTNFLT' THEN
					'MTN'
			ELSE
				CASE WHEN ProductGroup = 'SENFLT' THEN
					'SEN'
				ELSE
					CASE WHEN ProductGroup = 'SUBFLT' THEN
						'SUB'
					ELSE
						ProductGroup
					END
				END
			END, 
			AD.Banding;

END

GO
/****** Object:  StoredProcedure [dbo].[MRU_FO_PrimeSecondary_Balances_MakeTable]    Script Date: 2/11/2023 1:33:40 pm ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


CREATE PROCEDURE [dbo].[MRU_FO_PrimeSecondary_Balances_MakeTable]
AS
BEGIN

	SET NOCOUNT ON;

/********************************************************************************************************
 * This populates a miscellaneous table that stores liquid asset detail and not used in regulatory calculation.
 *
 ********************************************************************************************************
 *
 * @version 1.0
 * @author	Stephen Chin
 * @date	2020.08.13
 * 
 * - Start version control
 *
 *
*/

TRUNCATE TABLE MRU_FO_PrimeSecondary_Balances;
	
INSERT	MRU_FO_PrimeSecondary_Balances

SELECT	EODDate, [Type], Sector, Rating, [Settle Status], RepoType, [Deal Id], [Deal No], [Transaction Type], 
		Instrument, EndDate, DaysToMaturity, BucketProfileCashflow, NZDFaceValue_Bracketed, NZDMarketValue_Bracketed, 
		Tier1Haircut, Tier2Haircut, HaircutBucket, HaircutFactor, NZDHaircutMarketValue
	
FROM	TMSLiquids
	
WHERE	[Type] IN ('Prime Liquidity', 'Secondary Liquidity')
		AND [Settle Status] = 'Settled' 
		AND NZDFaceValue <> '0' 
		AND NZDMarketValue <> '0';
END

GO
/****** Object:  StoredProcedure [dbo].[MRU_FO_PrimeSecondary_Deals_MakeTable]    Script Date: 2/11/2023 1:33:40 pm ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[MRU_FO_PrimeSecondary_Deals_MakeTable]
	
AS
BEGIN
	
SET NOCOUNT ON;

/********************************************************************************************************
 * This table stores summary of (settled) liquid assets for forecasting tool.
 * 
 ********************************************************************************************************
 *
 * @version 1.0
 * @author	Stephen Chin
 * @date	2020.08.13
 * 
 * - Start version control
 * 
 *
*/

TRUNCATE TABLE MRU_FO_PrimeSecondary_Deals;

INSERT	MRU_FO_PrimeSecondary_Deals
		(EODDate, [Type], Sector, Rating, [Settle Status], RepoType, [Deal Id], [Deal No], [Transaction Type], Instrument, Entity, 
		Counterparty, [Issuer Name], BeginDate, EndDate, DaysToStart, StartBucket, DaysToMaturity, BucketProfileCashflow, NZDFaceValue, 
		NZDMarketValue, Tier1Haircut, Tier2Haircut, HaircutBucket, HaircutFactor, NZDHaircutMarketValue)

SELECT	EODDate, [Type], Sector, Rating, [Settle Status], RepoType, [Deal Id], [Deal No], [Transaction Type], Instrument, Entity, 
		Counterparty, [Issuer Name], BeginDate, EndDate, DaysToStart, StartBucket, DaysToMaturity, BucketProfileCashflow, NZDFaceValue, 
		NZDMarketValue, Tier1Haircut, Tier2Haircut, HaircutBucket, HaircutFactor, NZDHaircutMarketValue
FROM	TMSLiquids
WHERE	[Type] IN ('Prime Liquidity', 'Secondary Liquidity')
		AND [Settle Status] = 'Settled' 
		AND NZDFaceValue <> '0' 
		AND NZDMarketValue <> '0';

END

GO
/****** Object:  StoredProcedure [dbo].[MRU_FO_TMS_Cashflows_MakeTable]    Script Date: 2/11/2023 1:33:40 pm ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[MRU_FO_TMS_Cashflows_MakeTable]
AS

BEGIN
SET NOCOUNT ON;

/********************************************************************************************************
 * This populates a miscellaneous table that stores TMS cashflow detail and not used in regulatory calculation.
 *
 ********************************************************************************************************
 *
 * @version 1.0
 * @author	Stephen Chin
 * @date	2020.08.13
 * 
 * - Start version control
 *
 *
*/

TRUNCATE TABLE MRU_FO_TMS_Cashflows;
	
INSERT		MRU_FO_TMS_Cashflows

SELECT		CF.[Flow Type], 
			CF.[Type], 
			CASE WHEN CF.Sector = 'GovtIIBond' THEN
				'GovtBond'
			ELSE
				CF.Sector
			END AS Sector, 
			CF.BucketProfileCashflow, 
			SUM(CF.NZDTotalCashflow) AS SumOfNZDTotalCashflow 
	
FROM		TMSCashflows CF
	
WHERE		CF.[Settle Status] = 'Settled'
	
GROUP BY	CF.[Flow Type], 
			CF.[Type], 
			CASE WHEN CF.Sector = 'GovtIIBond' THEN
				'GovtBond'
			ELSE
				CF.Sector
			END, 
			CF.BucketProfileCashflow
	
ORDER BY	CF.[Flow Type], 
			CF.[Type], 
			CASE WHEN CF.Sector = 'GovtIIBond' THEN
				'GovtBond'
			ELSE
				CF.Sector
			END, 
			CF.BucketProfileCashflow;
	
END

GO
/****** Object:  StoredProcedure [dbo].[MRU_FO_Unsettled_Deals_MakeTable]    Script Date: 2/11/2023 1:33:40 pm ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[MRU_FO_Unsettled_Deals_MakeTable]
	
AS
BEGIN	
SET NOCOUNT ON;

/********************************************************************************************************
 * This table stores summary of unsettled liquid assets for forecasting tool.
 * 
 ********************************************************************************************************
 *
 * @version 1.0
 * @author	Stephen Chin
 * @date	2020.08.13
 * 
 * - Start version control
 * 
 *
*/

TRUNCATE TABLE MRU_FO_Unsettled_Deals;

INSERT		MRU_FO_Unsettled_Deals
			(EODDate, [Type], Sector, Rating, [Settle Status], RepoFlag, [Deal Id], [Deal No], [Transaction Type], Instrument, 
			Entity, Counterparty, [Issuer Name], BeginDate, EndDate, DaysToStart, StartBucket, DaysToMaturity, 
			BucketProfileCashflow, NZDFaceValue, NZDMarketValue, Tier1Haircut, Tier2Haircut, HaircutBucket, HaircutFactor, NZDHaircutMarketValue, NZDSettlementValue)
	
SELECT		EODDate, [Type], Sector, Rating, [Settle Status], RepoFlag, [Deal Id], [Deal No], [Transaction Type], Instrument, 
			Entity, Counterparty, [Issuer Name], BeginDate, EndDate, DaysToStart, StartBucket, DaysToMaturity, 
			BucketProfileCashflow, NZDFaceValue, NZDMarketValue, Tier1Haircut, Tier2Haircut, HaircutBucket, HaircutFactor, NZDHaircutMarketValue, NZDSettlementValue
	
FROM		TMSLiquids
	
WHERE		[Type] <> 'Internal' 
			AND [Settle Status] = 'Unsettled'
	
ORDER BY	[Type], Sector, Rating;

END

GO
/****** Object:  StoredProcedure [dbo].[MRU_Liquidity_FBDB_ReassignedClientType_MakeTable_K62]    Script Date: 2/11/2023 1:33:40 pm ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[MRU_Liquidity_FBDB_ReassignedClientType_MakeTable_K62]
AS
BEGIN
	
	SET NOCOUNT ON;

	/********************************************************************************************************
	 *
	 * General Scoping statement :
	 *
	 * ANZSICs codes scope is the  ’business customer class’ (ie. entities or non-individuals). 
	 * This includes trusts and small businesses, many of which are customers of the retail division.  
	 *
	 * Procedure scope: Liquidity Remediation for 
	 * (1) customerClassName like 'bus%'	 
	 * (2) ANZSIC code is like 'K62%'
	 * (3) clientType is null
	 * 
	 * Procedure actoions:
	 * (1) Updates table MRU_Liquidity_Funding_Balances_Details_Bandings, 
	 * (2) setting  ClientType to FI 
	 *
	 * Procedure downstream impact:
	 * (1) changing Clienttype from NULL to FI ensures that the funding will be classified as Market Funding
	 *
	 ********************************************************************************************************
	 *
	 * @version 1.17 - System Change SQL  – Schedule Flash Report
	 * @author	William Hsiao
	 * @date	2023.07.07
	 * 
	 * Replaced Manually run queries in Access by creating procedures to be scheduled
	 * 
	 *
	*/
	
	-- Records Business Clients with K62

	DELETE 
		FROM	MRU_Liquidity_FBDB_ReassignedClientType
		WHERE	ReassignedClass = 'K62_CLIENTTYPE_NULL';

	INSERT 
		INTO	MRU_Liquidity_FBDB_ReassignedClientType
		SELECT	'K62_CLIENTTYPE_NULL' 
				,FBD.*
				-- Adding two NULL to accomodate Apr28_2 noting PRD FBDB compared with seems to have two more columns that are CCYCarryValue, CarryValue
--				,NULL
--				,NULL
				-- end of / Remove when procedure uploaded into PRD
		
		FROM	[dbo].[MRU_Liquidity_Funding_Balances_Details_Bandings] FBD  
		WHERE   FBD.ClientType is null
			AND FBD.AccountNo in (
					SELECT  cast([AccessNumber] as nvarchar) 
					FROM	[SDT].[CRM_Customer]
					WHERE	ANZSIC2006 like 'K62%' AND CustomerClassName like 'bus%'
					);

	-- Update ClientType to FI in Liquidity FBDB
	UPDATE		[dbo].[MRU_Liquidity_Funding_Balances_Details_Bandings]
		SET		ClientType = 'FI'
		FROM	[dbo].[MRU_Liquidity_Funding_Balances_Details_Bandings] FBD
		WHERE	FBD.ClientType is null
			AND FBD.AccountNo in (
					SELECT  cast([AccessNumber] as nvarchar ) 
					FROM	[SDT].[CRM_Customer]
					WHERE	ANZSIC2006 like 'K62%' AND CustomerClassName like 'bus%'
					);

END;
GO
/****** Object:  StoredProcedure [dbo].[MRU_Liquidity_FBDB_ReassignedClientType_MakeTable_MissingANZSIC]    Script Date: 2/11/2023 1:33:40 pm ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[MRU_Liquidity_FBDB_ReassignedClientType_MakeTable_MissingANZSIC]
AS
BEGIN
	
	SET NOCOUNT ON;

	/********************************************************************************************************
	 *
	 * General Scoping statement :
	 *
	 * ANZSICs codes scope is the  ’business customer class’ (ie. entities or non-individuals). 
	 * This includes trusts and small businesses, many of which are customers of the retail division.  
	 
	 * Procedure scope: Liquidity Remediation for 	
	 * (1) customerClassName like 'BUS%' or missing or blank
     * (2) ANZSIC is missing or blank or ANZSIC is invalid in some way
	 * (3) clientCode is different from FI
     *	 
	 * Procedure actoions:
	 * (1) Updates table MRU_Liquidity_Funding_Balances_Details_Bandings, 
	 * (2) setting  ClientType to FI 
	 *
	 * Procedure downstream impact:
	 * (1) changing Clienttype from NULL to FI ensures that the funding will be classified as Market Funding
     *	 
	 ********************************************************************************************************
	 *
	 * @version 1.17 - System Change SQL – Schedule Flash Report
	 * @author	William Hsiao
	 * @date	2023.07.07
	 * 
	 * Replaced Manually run queries in Access by creating procedures to be scheduled
	 * 
	 *
	*/
	
	-- Records Business Clients with Missing ANZSIC code

	DELETE 
		FROM	MRU_Liquidity_FBDB_ReassignedClientType
		WHERE	ReassignedClass = 'MISSING_ANZSIC';

	INSERT 
		INTO	MRU_Liquidity_FBDB_ReassignedClientType
		SELECT   'MISSING_ANZSIC' 
				,FBD.*
				-- Adding two NULL to accomodate Apr28_2 noting PRD FBDB compared with seems to have two more columns that are CCYCarryValue, CarryValue
--				,NULL
--				,NULL
				-- end of / Remove when procedure uploaded into PRD

		FROM	[dbo].[MRU_Liquidity_Funding_Balances_Details_Bandings] FBD
					INNER JOIN [SDT].[CRM_Customer]
					ON cast (AccessNumber as nvarchar) = AccountNo
		WHERE	ISNULL(FBD.ClientType, 'NULL') <> 'FI'
			AND (ISNULL(ANZSICCode, 'MISSING') = 'MISSING' 
					OR ISNULL(ANZSICCode, 'MISSING') = '' 
					OR ISNUMERIC (ANZSICCode) = 1
					OR UPPER(ISNULL(ANZSICCode, 'MISSING')) like 'T%'
					)
			AND	(ISNULL(CustomerClassName , 'MISSING') = 'MISSING' 
					OR ISNULL(CustomerClassName , 'MISSING') = ''
					OR UPPER(ISNULL(CustomerClassName , 'MISSING')) like '%BUS%'
					);

	-- Update ClientType to FI in Liquidity FBDB
	--
	UPDATE		[dbo].[MRU_Liquidity_Funding_Balances_Details_Bandings]
		SET		ClientType = 'FI'
		FROM	[dbo].[MRU_Liquidity_Funding_Balances_Details_Bandings] FBD
					INNER JOIN [SDT].[CRM_Customer]
					ON cast (AccessNumber as nvarchar) = AccountNo
		WHERE	ISNULL(FBD.ClientType, 'NULL') <> 'FI'
			AND (ISNULL(ANZSICCode, 'MISSING') = 'MISSING' 
					OR ISNULL(ANZSICCode, 'MISSING') = '' 
					OR ISNUMERIC (ANZSICCode) = 1
					OR UPPER(ISNULL(ANZSICCode, 'MISSING')) like 'T%'
					)
			AND	(ISNULL(CustomerClassName , 'MISSING') = 'MISSING' 
					OR ISNULL(CustomerClassName , 'MISSING') = ''
					OR UPPER(ISNULL(CustomerClassName , 'MISSING')) like '%BUS%'
					);
END
GO
/****** Object:  StoredProcedure [dbo].[MRU_Liquidity_Funding_Balances_Details_Bandings_MakeTable]    Script Date: 2/11/2023 1:33:40 pm ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


CREATE PROCEDURE [dbo].[MRU_Liquidity_Funding_Balances_Details_Bandings_MakeTable]
       
AS
BEGIN  
SET NOCOUNT ON;

/********************************************************************************************************
 * This populates an interim table MRU_Liquidity_Funding_Balances_Details_Bandings with additional fields of
 * product, client total balance and size band on basis of MRU_Liquidity_Funding_Balances_Details.
 *
 ********************************************************************************************************
 *
 * - Start version control
 *
 * @version 1.0
 * @author	Stephen Chin
 * @date	2020.08.13
 * 
 * @version 1.2 - Funding value - SQL 1.11: Mapping face value to Funding value & adding Carrying Value and CCYCarrying Value
 * @author  My Phan
 * @date    2022.05.24
 *	- Adding new columns of carrying value
 		CASE WHEN (Product LIKE 'S8%' AND FBD.Amount IS NOT NULL) THEN FBD.Amount ELSE FBD.CCYCarryingValue END AS CCYCarryingValue,
		CASE WHEN (Product LIKE 'S8%' AND FBD.Amount IS NOT NULL) THEN FBD.Amount ELSE FBD.CarryingValue END AS CarryingValue
 *
 *
*/

DELETE FROM MRU_Liquidity_Funding_Balances_Details_Bandings;

INSERT	MRU_Liquidity_Funding_Balances_Details_Bandings

SELECT	FBD.EodDate,
		FBD.GroupAccountNo,
		FBD.AccountNo,
		FBD.AccountName,
		ISNULL(RCM.ClientType, ISNULL(CG.ClientType, TCM.ClientType)) AS ClientType,
		FBD.Product,
		FBD.ProductGroup,
		FBD.Residency,
		PM.TradeableFlag, 
		PM.Sector, 
		PM.ProductType, 
		CB.Banding, 
		FBD.LodegementDate,
		FBD.MaturityDate,
		FBD.IntFreq,
		FBD.Term,
		FBD.Rate,
		CASE WHEN (Product LIKE 'S8%' AND FBD.Amount IS NOT NULL) THEN FBD.Amount ELSE FBD.CCYBalance END AS CCYBalance,
		CASE WHEN (Product LIKE 'S8%' AND FBD.Amount IS NOT NULL) THEN FBD.Amount ELSE FBD.Balance END AS Balance,
		CASE WHEN (Product LIKE 'S8%' AND FBD.Amount IS NOT NULL) THEN FBD.Amount ELSE FBD.CCYFaceValue END AS CCYFaceValue,
		CASE WHEN (Product LIKE 'S8%' AND FBD.Amount IS NOT NULL) THEN FBD.Amount ELSE FBD.FaceValue END AS FaceValue,
		CASE WHEN (Product LIKE 'S8%' AND FBD.Amount IS NOT NULL) THEN FBD.Amount ELSE FBD.CCYBookValue END AS CCYBookValue,
		CASE WHEN (Product LIKE 'S8%' AND FBD.Amount IS NOT NULL) THEN FBD.Amount ELSE FBD.BookValue END AS BookValue,
		CASE WHEN (Product LIKE 'S8%' AND FBD.Amount IS NOT NULL) THEN FBD.Amount ELSE FBD.CCYMarketValue END AS CCYMarketValue,
		CASE WHEN (Product LIKE 'S8%' AND FBD.Amount IS NOT NULL) THEN FBD.Amount ELSE FBD.MarketValue END AS MarketValue,
		FBD.CCY,
		FBD.FXSpotFactor,
		CB.Balance AS ClientTotalBalance,
		FBD.Source,
		FBD.Identification,
		--v1.2 add 2 new variables
		CASE WHEN (Product LIKE 'S8%' AND FBD.Amount IS NOT NULL) THEN FBD.Amount ELSE FBD.CCYCarryingValue END AS CCYCarryingValue,
		CASE WHEN (Product LIKE 'S8%' AND FBD.Amount IS NOT NULL) THEN FBD.Amount ELSE FBD.CarryingValue END AS CarryingValue
FROM	MRU_Liquidity_Funding_Balances_Details FBD
		LEFT JOIN MRU_TMS_Client_Mapping TCM ON FBD.AccountNo = TCM.AccountNo
		LEFT JOIN MRU_Retail_ClientType_Mapping RCM ON FBD.AccountNo = RCM.AccountNo
		LEFT JOIN MRU_Product_Mapping PM ON FBD.ProductGroup = PM.ProductGrp
		LEFT JOIN MRU_Client_Bandings CB ON FBD.GroupAccountNo = CB.GroupAccountNo
		LEFT JOIN ClientGrouping CG ON FBD.GroupAccountNo = CG.GroupAccountNo AND FBD.AccountNo = CG.AccountNo;

END

GO
/****** Object:  StoredProcedure [dbo].[MRU_Liquidity_Funding_Balances_Details_MakeTable]    Script Date: 2/11/2023 1:33:40 pm ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[MRU_Liquidity_Funding_Balances_Details_MakeTable]  
AS
BEGIN  

	/**************************************************************************************************
	** This merges retail and wholesale funding balances.                                            **
	** Afterwards adjustment for Notice Saver (on-notice and not-on-notice) and retail term deposit  **
	** (contractual and at-risk) are followed.                                                       **
	**                                                                                               **
	***************************************************************************************************
	**                                                                                               **
	** @version 1.0                                                                                  **
	** @author	Stephen Chin                                                                         **
	** @date	2020.08.13                                                                           **
	** Start version control                                                                         ** 
	** Replace where source in TMS block with TMSDeals view to reduce operational risk and mainten-  **       
	** ance cost                                                                                     ** 
	** _____________________________________________________________________________________________ **
	**                                                                                               **
	** @version 1.2 - Funding value                                                                  **  
	** @author  My Phan                                                                              **
	** @date    2022.05.24                                                                           **
	** - Adding new column for carrying value into Retail, TMS and Breakable Deposit                 **
	**		- Retail                                                                                 **
	**				SUM(CASE WHEN FB.Product LIKE 'I%' THEN TD.Amount ELSE FB.EOD_CCYBalance END)    **
	**                  AS CCYCarryingValue,                                                         **
	**				SUM(CASE WHEN FB.Product LIKE 'I%' THEN TD.Amount ELSE FB.EOD_Balance END)       **
	**                  AS CarryingValue                                                             **
	**		- TMS                                                                                    **
	**				EOD.EodBalance AS CCYCarryingValue,                                              **
	**				EOD.EodBalance AS CarryingValue                                                  **
	**		- Breakable deposit                                                                      **
	**				SUM(CASE WHEN FB.Product LIKE 'I%' THEN TD.Amount ELSE FB.EOD_CCYBalance END)    **
	**                  AS CCYCarryingValue,                                                         **
	**				SUM(CASE WHEN FB.Product LIKE 'I%' THEN TD.Amount ELSE FB.EOD_Balance END)       **
	**                  AS CarryingValue                                                             **
	** _____________________________________________________________________________________________ **
	**                                                                                               **
	** @version  : 1.3                                                                               **
	** @location : S:\dept\Finance\Market Risk and Wholesale Accounting\Model\01_Liquidity\SQL\      **
	** 	           1.14.a Joint Accounts\Final SQL - send to IT (v1.14.a)                            **
	** @filename : 2.PROC_MRU_Liquidity_Funding_Balances_Details_MakeTable - v1 (of 1.14.a).sql      **
	** @author   : SP Barnarde                                                                       **
	** @date     : 20230621                                                                          **
	**                                                                                               **
	** Background:                                                                                   **
	**                                                                                               **
	** Joint account processing 'moves' joint account product balances to the joint account holder   **
	** who has the highest asset value, and is done by reassigning that joint account balance        **
	** record's AccountNo field. The 'original' account number is stored into the field              **
	** AccountNo_org. The link between AccountNo and AccountNo_org is located in MRU_AccountsEOD.    **
	**                                                                                               **
	** Consequence of this:                                                                          **
	**                                                                                               **
	** Liquidity funding balances details processes by joining to the following tables/views and     ** 
	** should take into account that these are using the pre-reassigned account number               **
	**     - MRU_Liquidity_Funding_Balances_Retail_TD                                                **
	**     - S8PaymentSummary                                                                        **
	**                                                                                               **
	** Purpose of this version:                                                                      **
	**                                                                                               **
	** Adjusts the join criteria of the first part of the view, that is,                             **
	**     - Ultacs.AccountsEOD on  AccountNo, is replaced by                                        **
	**     - MRU_AccountsEOD on AccountNo_org.                                                       **
	**                                                                                               **
	** As well, given the procedure estalbishes the bandings details in largely one big step, and    **
	** and that the join criteria are a little different for some products (due to reassignment),    **
	** the procedure is split into a number of additional INSERTS, for each of the product groups    **
	**                                                                                               **
	** As well, wanting to keep the pattern of the original query, the breakdown keeps the non-requ- **
	** ired tables in the join, and effectively removes them by way where 1<>1 selection criteria.   **
	** This is useful when comparing sql across the queries for common elements.                     **
	**                                                                                               **
	** _____________________________________________________________________________________________ **
	**                                                                                               **
	** @version  : 1.4                                                                              **
	** @location : S:\dept\Finance\Market Risk and Wholesale Accounting\Model\01_Liquidity\SQL\      **
	** 	           1.19 Undrawn Limit, Pipeline Aggregation, Joint Account\                          **
	**             Final SQL - send to IT (v1.19)                                                    **
	** @filename : 2..                                                                               **
	** @author   : SP Barnarde                                                                       **
	** @date     : 2023.10.17                                                                        **
	**                                                                                               **
	** Background:                                                                                   **
	**                                                                                               **
	** 		Version 1.18 introduced s8 balances in excess of EODBalance.                             **
	**                                                                                               **
	** Purpose of this version:                                                                      **
	**                                                                                               **
	**		Ensures that S8 balances align to EOD balance, and that S8 maturity buckets aligned to   **
	**      results under those of where joint account balances ar enot reassigned.                  **
	**                                                                                               **	
	**                                                                                               **
	** ************************************************************************************************
	*/

	SET NOCOUNT ON;


	DELETE 	MRU_Liquidity_Funding_Balances_Details;

	DECLARE @reportDate datetime; 
	SET		@reportDate = (Select ReportEndDate From Variables);
		 

	-- ------------------------------------------------------------------------------------------------
	-- ------------------------------------------------------------------------------------------------
	-- STEP 1:
	--
	-- v1.3, first insert is for, Retail TD's, I%
	--
	-- v1.4, clean out dependencies not required,
	--
	--	1.) Previous version split out certain of the INSERTS, and kept the functional form 
	--      identical between each insert, this version cleans up on that code
	--
	--		a.) remove any joins not required
	--		b.) remove any subsequent references not required
	--
	-- ------------------------------------------------------------------------------------------------
	-- ------------------------------------------------------------------------------------------------

	INSERT	MRU_Liquidity_Funding_Balances_Details

			-- it is preferred that columns being inserted as spcified, do that next verison change
			--

			SELECT		FB.EodDate				AS EodDate,
						FB.GroupAccountNo		AS GroupAccountNo,
						FB.AccountNo			AS AccountNo,
						
						CASE WHEN AST.AccountName IS NULL THEN CASE WHEN CS.Forenames IS NULL THEN CS.Surname ELSE LEFT(CS.Forenames,1) + ' ' + CS.Surname END ELSE AST.AccountName END					
												AS AccountName,
						
						FB.Product				AS Product, 
						dbo.GetProductGroup(FB.Product) AS ProductGroup, 
						
						CASE WHEN CS.WHtaxExempt = '2' THEN 'Offshore' ELSE 'Domestic' END					
												AS Residency, 
						
						TD.LodegementDate		AS LodegementDate, 
						TD.MaturityDate			AS MaturityDate,
						TD.IntFreq				AS IntFreq,
						TD.Term					AS Term, 
						TD.Rate					AS Rate, 

						TD.Amount				AS Amount, 
						SUM(TD.Amount)			AS CCYBalance, 
						SUM(TD.Amount)			AS Balance, 
						SUM(TD.Amount)			AS CCYFaceValue, 
						SUM(TD.Amount)			AS FaceValue, 
						SUM(TD.Amount)			AS CCYBookValue, 
						SUM(TD.Amount)			AS BookValue, 
						SUM(TD.Amount)			AS CCYMarketValue, 
						SUM(TD.Amount)			AS MarketValue,

						FB.CCY					AS CCY, 
						FB.FXSpotFactor			AS FXSpotFactor, 
						FB.SumOfEODLimit		AS SumOfEODLimit, 
						'Retail'				AS Source,
						TD.TDNo					AS Identification,
						TD.BreakFlag			AS TDBreakFlag,
						NULL					AS S8MaturityDate,
						NULL					AS S8BalPerMaturity,
						NULL					AS S8BalPerProduct,

						--- v1.2 add 2 new columns
						SUM(TD.Amount)			AS CCYCarryingValue, 
						SUM(TD.Amount)			AS CarryingValue 

						-- v1.3
						-- restrict the funding balances to only Term Deposits
			FROM		(Select * From MRU_Liquidity_Funding_Balances Where Product LIKE 'I%') FB

						-- v1.3 
						-- introduce this join to obtain original account number, and thus resolve TD join
						LEFT JOIN MRU_AccountsEOD MRUEOD 
								ON FB.AccountNo = MRUEOD.AccountNo 
									AND FB.Product = MRUEOD.Product AND MRUEOD.EODBalance <> 0

						LEFT JOIN AccountStatic AST ON FB.Product = AST.Product AND FB.AccountNo = AST.Account
						LEFT JOIN CustomerStatic CS ON FB.AccountNo = CS.AccessNo

						-- v1.3
						-- Complete the TD join, by using MRUAccountsEOD.AccountNo_org
						--LEFT JOIN (Select * From MRU_Liquidity_Funding_Balances_Retail_TD Where BreakFlag = 'N') TD ON FB.EodDate = TD.FileCreateDate AND FB.Product = TD.Product AND FB.AccountNo = TD.Accounts
						LEFT JOIN (Select * From MRU_Liquidity_Funding_Balances_Retail_TD Where BreakFlag = 'N') TD 
								ON FB.EodDate = TD.FileCreateDate 
									AND FB.Product = TD.Product AND MRUEOD.AccountNo_org = TD.Accounts

							
			WHERE		FB.EodDate = @reportDate
			
			GROUP BY	FB.EodDate, 
						FB.GroupAccountNo, 
						FB.AccountNo, 
						AST.AccountName, 
						FB.Product, 
						dbo.GetProductGroup(FB.Product), 
						CASE WHEN CS.WHtaxExempt = '2' THEN 'Offshore' ELSE 'Domestic' END,
						CS.Forenames,
						CS.Surname,
						TD.LodegementDate, 
						TD.MaturityDate, 
						TD.IntFreq, 
						TD.Term,
						TD.Rate, 
						TD.Amount,
						FB.CCY,
						FB.FXSpotFactor, 
						FB.SumOfEODLimit, 
						TD.TDNo,
						TD.BreakFlag;

	-- ------------------------------------------------------------------------------------------------
	-- ------------------------------------------------------------------------------------------------
	-- STEP 2:
	-- 
	-- v1.3, add the next product grouping, Retail S8
	--
	-- v1.4, clean out dependencies not required,
	--
	--	1.) Previous version split out certain of the INSERTS, and kept the functional form 
	--      identical between each insert, this version cleans up on that code
	--
	--		a.) remove any joins not required
	--		b.) remove any subsequent references not required
	--
	--	2.) Remove join to S8PaymentSummary, and address the notice saver calls seperately 
	--
	--		a.) This insert brings over the EODBalances from AccountEOD
	-- 		b.) Notice saver which are called, reduce funding at specific called dates, these effects
	--			are dealt with in STEP x
	--
	-- ------------------------------------------------------------------------------------------------
	-- ------------------------------------------------------------------------------------------------

	INSERT	MRU_Liquidity_Funding_Balances_Details

			-- it is preferred that columns being inserted as spcified, do that next verison change
			--

			SELECT		FB.EodDate				AS EodDate,
						FB.GroupAccountNo		AS GroupAccountNo,
						FB.AccountNo			AS AccountNo,

						CASE WHEN AST.AccountName IS NULL THEN CASE WHEN CS.Forenames IS NULL THEN CS.Surname ELSE LEFT(CS.Forenames,1) + ' ' + CS.Surname END ELSE AST.AccountName END
												AS AccountName,

						FB.Product				AS Product, 
						dbo.GetProductGroup(FB.Product) 
												AS ProductGroup, 

						CASE WHEN CS.WHtaxExempt = '2' THEN 'Offshore' ELSE 'Domestic' END
												AS Residency, 
						NULL					AS LodegementDate, 
						NULL					AS MaturityDate,
						NULL					AS IntFreq,
						NULL					AS Term, 
						NULL					AS Rate, 

						NULL					AS Amount,
						SUM(FB.EOD_CCYBalance)	AS CCYBalance, 
						SUM(FB.EOD_Balance)		AS Balance, 
						SUM(FB.EOD_CCYBalance)	AS CCYFaceValue, 
						SUM(FB.EOD_Balance)		AS FaceValue, 
						SUM(FB.EOD_CCYBalance)	AS CCYBookValue, 
						SUM(FB.EOD_Balance)		AS BookValue, 
						SUM(FB.EOD_CCYBalance)	AS CCYMarketValue, 
						SUM(FB.EOD_Balance)		AS MarketValue,

						FB.CCY					AS CCY, 
						FB.FXSpotFactor			AS FXSpotFactor, 
						FB.SumOfEODLimit		AS SumOfEODLimit, 
						'Retail'				AS Source,
						NULL					AS Identification,
						NULL					AS TDBreakFlag,
						NULL					AS S8MaturityDate,
						NULL					AS S8BalPerMaturity,
						NULL					AS S8BalPerProduct,

						--- v1.2 add 2 new columns
						SUM(FB.EOD_CCYBalance)	AS CCYCarryingValue, 
						SUM(FB.EOD_Balance)		AS CarryingValue 

						-- v1.3
						-- restrict the funding balances to only S8% products

			FROM		(Select * From MRU_Liquidity_Funding_Balances Where Product LIKE 'S8%') FB

						LEFT JOIN (SELECT DISTINCT * FROM AccountStatic) AST ON FB.Product = AST.Product AND FB.AccountNo = AST.Account
						LEFT JOIN (SELECT DISTINCT * FROM CustomerStatic) CS ON FB.AccountNo = CS.AccessNo

			WHERE		FB.EodDate = @reportDate
			
			GROUP BY	FB.EodDate, 
						FB.GroupAccountNo, 
						FB.AccountNo, 
						AST.AccountName, 
						FB.Product, 
						dbo.GetProductGroup(FB.Product), 
						CASE WHEN CS.WHtaxExempt = '2' THEN 'Offshore' ELSE 'Domestic' END,
						CS.Forenames,
						CS.Surname,
						--TD.LodegementDate, 
						--TD.MaturityDate, 
						--TD.IntFreq, 
						--TD.Term,
						--TD.Rate, 
						--TD.Amount,
						FB.CCY,
						FB.FXSpotFactor, 
						FB.SumOfEODLimit 
						--TD.TDNo,
						--TD.BreakFlag,
						--NS.AccessNo,
						--NS.Product,
						--NS.S8MaturityDate,
						--NS.S8BalPerMaturity,
						--NS.S8BalPerProduct
						;

	-- ------------------------------------------------------------------------------------------------
	-- ------------------------------------------------------------------------------------------------
	-- STEP 3:
	--
	-- v1.3, add the next product grouping, the remainder of retail
	--
	-- v1.4, clean out dependencies not required,
	--
	--	1.) Previous version split out certain of the INSERTS, and kept the functional form 
	--      identical between each insert, this version cleans up on that code
	--
	--		a.) remove any joins not required
	--		b.) remove any subsequent references not required
	
	-- ------------------------------------------------------------------------------------------------
	-- ------------------------------------------------------------------------------------------------

	INSERT	MRU_Liquidity_Funding_Balances_Details

			-- it is preferred that columns being inserted as spcified, do that next verison change
			--

			SELECT		FB.EodDate				AS EodDate,
						FB.GroupAccountNo		AS GroupAccountNo,
						FB.AccountNo			AS AccountNo,
						CASE WHEN AST.AccountName IS NULL THEN CASE WHEN CS.Forenames IS NULL THEN CS.Surname ELSE LEFT(CS.Forenames,1) + ' ' + CS.Surname END ELSE AST.AccountName END					
												AS AccountName,
						FB.Product				AS Product, 
						dbo.GetProductGroup(FB.Product) 
												AS ProductGroup, 
						CASE WHEN CS.WHtaxExempt = '2' THEN 'Offshore' ELSE 'Domestic' END					
												AS Residency, 
						
						--TD.LodegementDate		AS LodegementDate, 
						NULL					AS LodegementDate, 
						
						--CASE WHEN FB.Product LIKE 'I%' THEN TD.MaturityDate ELSE CASE WHEN FB.Product LIKE 'S8%' THEN NS.S8MaturityDate ELSE TD.MaturityDate END END
						NULL					AS MaturityDate,
						--TD.IntFreq				AS IntFreq,
						--TD.Term					AS Term, 
						--TD.Rate					AS Rate, 
						NULL					AS IntFreq,
						NULL					AS Term, 
						NULL					AS Rate, 

						
						--CASE WHEN FB.Product LIKE 'I%' THEN TD.Amount ELSE CASE WHEN FB.Product LIKE 'S8%' THEN NS.S8BalPerMaturity ELSE TD.Amount END END
						NULL					AS Amount, 
						
						SUM(FB.EOD_CCYBalance)	AS CCYBalance, 
						SUM(FB.EOD_Balance)		AS Balance, 
						SUM(FB.EOD_CCYBalance)	AS CCYFaceValue, 
						SUM(FB.EOD_Balance)		AS FaceValue, 
						SUM(FB.EOD_CCYBalance)	AS CCYBookValue, 
						SUM(FB.EOD_Balance)		AS BookValue, 
						SUM(FB.EOD_CCYBalance)	AS CCYMarketValue, 
						SUM(FB.EOD_Balance)		AS MarketValue,
						FB.CCY					AS CCY, 
						FB.FXSpotFactor			AS FXSpotFactor, 
						FB.SumOfEODLimit		AS SumOfEODLimit, 
						'Retail'				AS Source,
						
						--TD.TDNo					AS Identification,
						--TD.BreakFlag			AS TDBreakFlag,
						NULL					AS Identification,
						NULL					AS TDBreakFlag,
						
						NULL					AS S8MaturityDate,
						NULL					AS S8BalPerMaturity,
						NULL					AS S8BalPerProduct,
									
						--- v1.2 add 2 new columns
						SUM(FB.EOD_CCYBalance)	AS CCYCarryingValue, 
						SUM(FB.EOD_Balance)		AS CarryingValue 

						-- v1.3
						-- restrict the funding balances to  products other than S8% and I%
			FROM		(Select * From MRU_Liquidity_Funding_Balances Where Product NOT LIKE 'S8%' AND Product NOT LIKE 'I%') FB

						LEFT JOIN AccountStatic AST ON FB.Product = AST.Product AND FB.AccountNo = AST.Account
						LEFT JOIN CustomerStatic CS ON FB.AccountNo = CS.AccessNo

						-- v1.3
						-- keep the pattern of the original join in place by introducing a null dataset
						/*
						LEFT JOIN (Select * From MRU_Liquidity_Funding_Balances_Retail_TD Where BreakFlag = 'N' 
							AND 1<>1) TD 
								ON FB.EodDate = TD.FileCreateDate 
									AND FB.Product = TD.Product AND FB.AccountNo = TD.Accounts
						*/
						-- v1.3
						-- keep the pattern of the original join in place by introducing a null dataset
						/*
						LEFT JOIN (SELECT * FROM S8PaymentSummary 
							WHERE 1<>1) NS 
								ON FB.AccountNo = NS.AccessNo AND FB.Product = NS.Product
						*/
						
			WHERE		FB.EodDate = @reportDate

			GROUP BY	FB.EodDate, 
						FB.GroupAccountNo, 
						FB.AccountNo, 
						AST.AccountName, 
						FB.Product, 
						dbo.GetProductGroup(FB.Product), 
						CASE WHEN CS.WHtaxExempt = '2' THEN 'Offshore' ELSE 'Domestic' END,
						CS.Forenames,
						CS.Surname,
						--TD.LodegementDate, 
						-- TD.MaturityDate, 
						-- TD.IntFreq, 
						-- TD.Term,
						-- TD.Rate, 
						-- TD.Amount,
						FB.CCY,
						FB.FXSpotFactor, 
						FB.SumOfEODLimit 
						-- TD.TDNo,
						-- TD.BreakFlag,
						-- NS.AccessNo,
						-- NS.Product,
						-- NS.S8MaturityDate,
						-- NS.S8BalPerMaturity,
						-- NS.S8BalPerProduct;
						;

	-- ------------------------------------------------------------------------------------------------
	-- ------------------------------------------------------------------------------------------------
	-- STEP 4:
	--
	-- v1.3, add the next product grouping, the remainder of retail
	--
	-- ------------------------------------------------------------------------------------------------
	-- ------------------------------------------------------------------------------------------------

	INSERT	MRU_Liquidity_Funding_Balances_Details

			-- it is preferred that columns being inserted as spcified, do that next verison change
			--

			SELECT		TMS.EodDate				AS EodDate,
						TMS.GroupAccountNo		AS GroupAccountNo,
						TMS.AccountNo			AS AccountNo,
						TMS.SourceCounterparty	AS AccountName,
						TMS.ProductGroup		AS Product,
						TMS.ProductGroup		AS ProductGroup,
						TMS.Residency			AS Residency,
						TMS.LodegementDate		AS LodegementDate,
						TMS.MaturityDate		AS MaturityDate,
						NULL					AS IntFreq,
						TMS.Term				AS Term,
						TMS.DealtRate			AS Rate,
						TMS.Amount				AS Amount,
						TMS.EOD_CCYBalance		AS CCYBalance, 
						TMS.EOD_Balance			AS Balance, 
						TMS.CCYFaceValue		AS CCYFaceValue, 
						TMS.FaceValue			AS FaceValue, 
						TMS.CCYBookValue		AS CCYBookValue, 
						TMS.BookValue			AS BookValue, 
						TMS.CCYMarketValue		AS CCYMarketValue, 
						TMS.MarketValue			AS MarketValue,		
						TMS.CCY					AS CCY, 
						TMS.FXSpotFactor		AS FXSpotFactor, 
						TMS.SumOfEODLimit		AS SumOfEODLimit, 
						TMS.[Source]			AS Source,
						TMS.DealNo				AS Identification,
						NULL					AS TDBreakFlag,
						NULL					AS S8MaturityDate,
						NULL					AS S8BalPerMaturity,
						NULL					AS S8BalPerProduct,

						---v1.2 add 2 new columns
						TMS.CCYCarryingValue	AS CCYCarryingValue, 
						TMS.CarryingValue		AS CarryingValue

			FROM		TMSDeals TMS;

	-- ------------------------------------------------------------------------------------------------
	-- ------------------------------------------------------------------------------------------------
	-- STEP 5:
	--
	-- v1.4, to cater for S8PaymentSummary, that is, notice save calls
	--
	-- 1.) for optimisation, create a temporary table of the noticae saver calls, and associating the
	--     notice samer accesno, to the reassigned accessno which occured in the balances reassignment 
	--     step.
	--
	-- 2.) Insert the notice saver calls, the amount and the maturity date
	--
	-- 3.) To adjust the EODBalance, from Step 2, to take account of the notice saver calls, repeat The
	--     insert from 2.) above, however, invert the sign, and dont insert a maturity date
	--
	-- ------------------------------------------------------------------------------------------------
	-- ------------------------------------------------------------------------------------------------

	-- STEP 5.1.a, create the temporrary table
	--
	DECLARE	@S8tmp AS TABLE (
				AccountNo		varchar(8), 
				Product			varchar(6), 
				S8MaturityDate	smalldatetime, 
				S8BalPerMaturity money,
				AccountName		varchar(150), 
				Forenames		varchar(120), 
				Surname			varchar(120),
				WHtaxExempt		varchar(20)
				);

	-- STEP 5.1.b, insert S8 summary into this temp table

	INSERT	@S8tmp 

			-- it is preferred that columns being inserted as spcified, do that next verison change
			--

			SELECT		S8.AccountNo, S8.Product, S8MaturityDate, S8BalPerMaturity, AST.AccountName, CS.Forenames, CS.Surname, WHtaxExempt
				FROM
				(	SELECT		MRUEOD.AccountNo		As AccountNo,
								S8.Product,
								S8MaturityDate,
								SUM(S8BalPerMaturity)   AS S8BalPerMaturity

						FROM	MRU_AccountsEOD MRUEOD,
								(
								SELECT		AccessNo, 
											Product, 
											S8MaturityDate			AS S8MaturityDate, 
											MIN(S8BalPerMaturity)	AS S8BalPerMaturity

									FROM	S8PaymentSummary
									GROUP BY	AccessNo, Product, S8MaturityDate
								) S8

						WHERE	MRUEOD.AccountNo_org = S8.AccessNo
							AND MRUEOD.Product = S8.Product
							AND MRUEOD.Product LIKE 'S8%'

						GROUP BY MRUEOD.AccountNo, S8.Product, S8MaturityDate		
				) S8

			LEFT JOIN (SELECT DISTINCT * FROM AccountStatic) AST ON S8.Product = AST.Product AND S8.AccountNo = AST.Account
			LEFT JOIN (SELECT DISTINCT * FROM CustomerStatic) CS ON S8.AccountNo = CS.AccessNo;

	-- Step 5.2, Insert the S8 payment & Date
	--

	INSERT	MRU_Liquidity_Funding_Balances_Details

			-- it is preferred that columns being inserted as spcified, do that next verison change
			--

			SELECT
			
						@ReportDate				AS EodDate,
						FB.GroupAccountNo		AS GroupAccountNo,
						FB.AccountNo			AS AccountNo,
						CASE WHEN AccountName IS NULL THEN CASE WHEN Forenames IS NULL THEN Surname ELSE LEFT(Forenames,1) + ' ' + Surname END ELSE AccountName END
												AS AccountName,
						FB.Product				AS Product, 
						dbo.GetProductGroup(FB.Product) 
												AS ProductGroup, 
						CASE WHEN WHtaxExempt = '2' THEN 'Offshore' ELSE 'Domestic' END
												AS Residency, 
						NULL					AS LodegementDate, 
						S8MaturityDate			AS MaturityDate,
						NULL					AS IntFreq,
						NULL					AS Term, 
						NULL					AS Rate, 

						NULL					AS Amount, 
						S8BalPerMaturity		AS CCYBalance, 
						S8BalPerMaturity		AS Balance, 
						S8BalPerMaturity		AS CCYFaceValue, 
						S8BalPerMaturity		AS FaceValue, 
						S8BalPerMaturity		AS CCYBookValue, 
						S8BalPerMaturity		AS BookValue, 
						S8BalPerMaturity		AS CCYMarketValue, 
						S8BalPerMaturity		AS MarketValue,

						'NZD'					AS CCY, 
						1						AS FXSpotFactor, 
						NULL					AS SumOfEODLimit, 
						'Retail'				AS Source,
						NULL					AS Identification,
						NULL					AS TDBreakFlag,
						S8MaturityDate			AS S8MaturityDate,
						S8BalPerMaturity		AS S8BalPerMaturity,
						NULL					AS S8BalPerProduct,

						--- v1.2 add 2 new columns
						S8BalPerMaturity		AS CCYCarryingValue, 
						S8BalPerMaturity		AS CarryingValue 

						-- v1.3
						-- restrict the funding balances to only S8% products

			FROM	
			
					(Select DISTINCT AccountNo, GroupAccountNo, Product From MRU_Liquidity_Funding_Balances Where Product LIKE 'S8%') FB
					LEFT JOIN @S8tmp S8 ON FB.AccountNo = S8.AccountNo AND FB.Product = S8.Product

			WHERE S8BalPerMaturity IS NOT NULL
			
	UNION ALL 

	-- Step 5.3, Reverse the amounts, without a maturity date
	--

			SELECT		@ReportDate				AS EodDate,
						FB.GroupAccountNo		AS GroupAccountNo,
						S8.AccountNo			AS AccountNo,
						CASE WHEN AccountName IS NULL THEN CASE WHEN Forenames IS NULL THEN Surname ELSE LEFT(Forenames,1) + ' ' + Surname END ELSE AccountName END
												AS AccountName,
						S8.Product				AS Product, 
						dbo.GetProductGroup(S8.Product) 
												AS ProductGroup, 
						CASE WHEN WHtaxExempt = '2' THEN 'Offshore' ELSE 'Domestic' END AS Residency, 

						NULL					AS LodegementDate, 
						NULL					AS MaturityDate,
						NULL					AS IntFreq,
						NULL					AS Term, 
						NULL					AS Rate, 

						NULL					AS Amount, 
						-S8BalPerMaturity		AS CCYBalance, 
						-S8BalPerMaturity		AS Balance, 
						-S8BalPerMaturity		AS CCYFaceValue, 
						-S8BalPerMaturity		AS FaceValue, 
						-S8BalPerMaturity		AS CCYBookValue, 
						-S8BalPerMaturity		AS BookValue, 
						-S8BalPerMaturity		AS CCYMarketValue, 
						-S8BalPerMaturity		AS MarketValue,

						'NZD'					AS CCY, 
						1						AS FXSpotFactor, 
						NULL					AS SumOfEODLimit, 
						'Retail'				AS Source,
						NULL					AS Identification,
						NULL					AS TDBreakFlag,
						NULL					AS S8MaturityDate,
						-S8BalPerMaturity		AS S8BalPerMaturity,
						NULL					AS S8BalPerProduct,

						--- v1.2 add 2 new columns
						-S8BalPerMaturity		AS CCYCarryingValue, 
						-S8BalPerMaturity		AS CarryingValue 

						-- v1.3
						-- restrict the funding balances to only S8% products

			FROM	
			
					(Select DISTINCT AccountNo, GroupAccountNo, Product From MRU_Liquidity_Funding_Balances Where Product LIKE 'S8%') FB
					LEFT JOIN @S8tmp S8 ON FB.AccountNo = S8.AccountNo AND FB.Product = S8.Product 
			
			WHERE	S8BalPerMaturity IS NOT NULL;

	
	-- v1.4, given the S8 step above, the data is clean, and the following delete is no longer required
	--
	
	------
	-- Delete duplicates of remaining balance row for every notice amount row (equivalently, Amount is not null) 
	-- generated in previous section as we only need one remaining Notice Saver balance per account and product.
	-- RN returns the sequential number of a row within a partition of a result set, starting at 1 for the first row in each partition.
	/*
	WITH CTE AS
			(
			SELECT 		*,
						RN = ROW_NUMBER() OVER (PARTITION BY EodDate, AccountNo, Product, MaturityDate, Amount ORDER BY EodDate, AccountNo, Product, MaturityDate, Amount)
			FROM   		MRU_Liquidity_Funding_Balances_Details
			WHERE  		Product = 'S8' OR Product LIKE 'S8.%'
			)

	DELETE FROM CTE WHERE RN > 1;
	*/

	-- STEP 5
	-- 
	-- Replace Amount and *Value columns with AccountsEOD balance if they are null.
	-- It is treated as at-call by assigning report date to LodgementDate and MaturityDate.
	--

	INSERT	MRU_Liquidity_Funding_Balances_Details

			-- it is preferred that columns being inserted as spcified, do that next verison change
			--

			SELECT		TMP.EodDate, 
						TMP.GroupAccountNo, 
						TMP.AccountNo, 
						TMP.AccountName, 
						TMP.Product, 
						TMP.ProductGroup, 
						TMP.Residency,

						@ReportDate				AS LodegementDate,
						@ReportDate				AS MaturityDate,
						NULL					AS IntFreq,
						NULL					AS Term,
						NULL					AS Rate,
						EOD.EodBalance			AS Amount,
						EOD.EodBalance			AS CCYBalance,
						EOD.EodBalance			AS Balance,
						EOD.EodBalance			AS CCYFaceValue,
						EOD.EodBalance			AS FaceValue,
						EOD.EodBalance			AS CCYBookValue,
						EOD.EodBalance			AS BookValue,
						EOD.EodBalance			AS CCYMarketValue,
						EOD.EodBalance			AS MarketValue,
						TMP.CCY,
						TMP.FXSpotFactor,
						TMP.SumOfEODLimit,
						TMP.Source,
						TMP.Identification,
						TMP.TDBreakFlag,
						TMP.S8MaturityDate,
						TMP.S8BalPerMaturity,
						TMP.S8BalPerProduct,
						
						---v1.2 add 2 new columns
						EOD.EodBalance			AS CCYCarryingValue,
						EOD.EodBalance			AS CarryingValue

						-- v1.3
						-- in the case of this join, no adjustment regarding AccountNo_org is required as
						-- these tables all relate to the same AccountNo.
						--					
			FROM		(Select * From MRU_Liquidity_Funding_Balances_Details Where Balance IS Null
			
			-- v1.4, do not apply this process to the S8% as the date has been cimpleted in previous steps
			--
							AND Product NOT Like 'S8%'
							) TMP
						LEFT JOIN MRU_Liquidity_Funding_Balances_Details FBD on TMP.AccountNo = FBD.AccountNo AND TMP.Product = FBD.Product
						LEFT JOIN Ultracs.AccountsEOD EOD ON TMP.AccountNo = EOD.AccountNo AND TMP.Product = EOD.Product;

	DELETE	MRU_Liquidity_Funding_Balances_Details 
			WHERE		Balance IS NULL;

	-- STEP 6
	--
	-- Append current breakable term deposit amount.
	-- Row with NULL amount is excluded as the missing TD spec has been already filled with EOD balance.
	--

	INSERT	MRU_Liquidity_Funding_Balances_Details

			-- it is preferred that columns being inserted as spcified, do that next verison change
			--

			SELECT		FB.EodDate				AS EodDate,
						FB.GroupAccountNo		AS GroupAccountNo,
						FB.AccountNo			AS AccountNo,
						
						CASE WHEN AST.AccountName IS NULL THEN CASE WHEN CS.Forenames IS NULL THEN CS.Surname ELSE LEFT(CS.Forenames,1) + ' ' + CS.Surname END ELSE AST.AccountName END
												AS AccountName,
						
						FB.Product				AS Product, 
						dbo.GetProductGroup(FB.Product) 
												AS ProductGroup, 
						CASE WHEN CS.WHtaxExempt = '2' THEN 'Offshore' ELSE 'Domestic' END
												AS Residency, 

						TD.LodegementDate		AS LodegementDate, 
						TD.MaturityDate			AS MaturityDate,
						TD.IntFreq				AS IntFreq,
						TD.Term					AS Term, 
						TD.Rate					AS Rate, 
						TD.Amount				AS Amount, 
						SUM(TD.Amount)			AS CCYBalance, 
						SUM(TD.Amount)			AS Balance, 
						SUM(TD.Amount)			AS CCYFaceValue, 
						SUM(TD.Amount)			AS FaceValue, 
						SUM(TD.Amount)			AS CCYBookValue, 
						SUM(TD.Amount)			AS BookValue, 
						SUM(TD.Amount)			AS CCYMarketValue, 
						SUM(TD.Amount)			AS MarketValue,
						FB.CCY					AS CCY, 
						FB.FXSpotFactor			AS FXSpotFactor, 
						FB.SumOfEODLimit		AS SumOfEODLimit, 
						'Retail'				AS Source,
						TD.TDNo					AS Identification,
						TD.BreakFlag			AS TDBreakFlag,
						NULL					AS S8MaturityDate,
						NULL					AS S8BalPerMaturity,
						NULL					AS S8BalPerProduct,
						
						--- v1.2 add 2 new columns
						SUM(TD.Amount)			AS CCYCarryingValue, 
						SUM(TD.Amount)			AS CarryingValue

			FROM		(Select * From MRU_Liquidity_Funding_Balances Where Product LIKE 'I%') FB
				
						-- v1.3 
						-- introduce this join to obtain original account number, and thus resolve TD join
						--
						LEFT JOIN MRU_AccountsEOD MRUEOD 
								ON FB.AccountNo = MRUEOD.AccountNo 
									AND FB.Product = MRUEOD.Product AND MRUEOD.EODBalance <> 0

						LEFT JOIN AccountStatic AST ON FB.Product = AST.Product AND FB.AccountNo = AST.Account
						LEFT JOIN CustomerStatic CS ON FB.AccountNo = CS.AccessNo

						-- v1.3
						-- Complete the TD join, by using MRUAccountsEOD.AccountNo_org
						--LEFT JOIN (Select * From MRU_Liquidity_Funding_Balances_Retail_TD Where BreakFlag = 'Y') TD ON FB.EodDate = TD.FileCreateDate AND FB.Product = TD.Product AND FB.AccountNo = TD.Accounts
						--
						LEFT JOIN (Select * From MRU_Liquidity_Funding_Balances_Retail_TD Where BreakFlag = 'Y') TD 
								ON FB.EodDate = TD.FileCreateDate 
									AND FB.Product = TD.Product AND MRUEOD.AccountNo_org = TD.Accounts


			WHERE		FB.EodDate = @reportDate
					AND Amount IS NOT NULL

									
			GROUP BY	FB.EodDate, 
						FB.GroupAccountNo, 
						FB.AccountNo, 
						AST.AccountName, 
						FB.Product, 
						dbo.GetProductGroup(FB.Product), 
						CASE WHEN CS.WHtaxExempt = '2' THEN 'Offshore' ELSE 'Domestic' END,
						CS.Forenames,
						CS.Surname,
						TD.LodegementDate, 
						TD.MaturityDate, 
						TD.IntFreq, 
						TD.Term,
						TD.Rate, 
						TD.Amount,
						FB.CCY,
						FB.FXSpotFactor, 
						FB.SumOfEODLimit, 
						TD.TDNo,
						TD.BreakFlag
						--NS.AccessNo,
						--NS.Product,
						--NS.S8MaturityDate,
						--NS.S8BalPerMaturity,
						--NS.S8BalPerProduct
						;

	END

GO
/****** Object:  StoredProcedure [dbo].[MRU_Liquidity_Funding_Balances_MakeTable]    Script Date: 2/11/2023 1:33:40 pm ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO





CREATE PROCEDURE [dbo].[MRU_Liquidity_Funding_Balances_MakeTable]	
AS
BEGIN
	
SET NOCOUNT ON;
/********************************************************************************************************
 * This populates an interim table MRU_Liquidity_Funding_Balances sourcing from retail system, Ultracs and B2K.
 * Product codes like S7*, S69%, S8[0-9]* and S9[0-9]* excluded since they are not external products. 
 * Aggregated NZD balance of Loaded for Travel assigns to account number 1047 with product code of S76 explicitly,
 * because individual account level of data is not available in Ultracs.
 *
 * Following account also excluded as per Finance (Blair Bradley) requested (due to Kiwibank internal bank account):
 * - Account number = 2345525 and (Product = S30 or S30.1 or S30.2 or S30.3)
 *
 ********************************************************************************************************
 *
 * @version 1.0
 * @author	Stephen Chin
 * @date	2020.08.13
 * 
 * - Start version control
 * - Add more product codes, S30.1/2/3 of account 2345525 to exclusion list
 * - Delete FCA (Cymonz) source as the release of International project has been cancelled.
 * ******************************************************************************************************
 * 
 * @version 1.2
 * @author My Phan
 * @date 2022.05.19
 * - explicitly exclusing S69 (Z ATM cash - branch cash) from funding side
 *
*/
		
TRUNCATE TABLE MRU_Liquidity_Funding_Balances;

DECLARE @reportDate DATETIME;
Set		@reportDate = (Select CONVERT(DATETIME, Variable, 103) From tblVariables Where No = 1);

INSERT		MRU_Liquidity_Funding_Balances

-- Ultracs
SELECT		EOD.EodDate AS EodDate, 
			ISNULL(CG.GroupAccountNo, EOD.AccountNo) AS GroupAccountNo, 
			EOD.AccountNo AS AccountNo, 
			EOD.Product AS Product, 
			SUM(EOD.EodBalance) AS EOD_Balance, 
			SUM(EOD.EodBalance) AS EOD_CCYBalance, 
			'NZD' AS CCY, 
			1.0 AS FXSpotFactor, 
			SUM(EOD.EODLimit) AS SumOfEODLimit
FROM		Ultracs.AccountsEOD EOD 
			LEFT JOIN ClientGrouping CG ON EOD.AccountNo = CG.AccountNo
WHERE		EOD.EodDate = @reportDate 
			AND EOD.Product NOT LIKE 'S7%'  AND EOD.Product NOT LIKE 'S69%' AND EOD.Product NOT LIKE 'S80%'
			AND EOD.Product NOT LIKE 'S81%' AND EOD.Product NOT LIKE 'S82%' AND EOD.Product NOT LIKE 'S83%'
			AND EOD.Product NOT LIKE 'S84%'	AND EOD.Product NOT LIKE 'S85%' AND EOD.Product NOT LIKE 'S86%'
			AND EOD.Product NOT LIKE 'S87%' AND EOD.Product NOT LIKE 'S88%' AND EOD.Product NOT LIKE 'S89%'
			AND EOD.Product NOT LIKE 'S91%' AND EOD.Product NOT LIKE 'S92%' AND EOD.Product NOT LIKE 'S93%'
			AND EOD.Product NOT LIKE 'S94%' AND EOD.Product NOT LIKE 'S95%' AND EOD.Product NOT LIKE 'S96%'
			AND EOD.Product NOT LIKE 'S97%' AND EOD.Product NOT LIKE 'S98%' AND EOD.Product NOT LIKE 'S99%'
GROUP BY	EOD.EodDate, 
			ISNULL(CG.GroupAccountNo, EOD.AccountNo), 
			EOD.AccountNo, 
			EOD.Product
HAVING		SUM(EOD.EodBalance) > 0

UNION ALL

-- B2K (credit card in-funds)
SELECT		CC.EODDate AS EodDate,
			ISNULL(CG.GroupAccountNo, CC.AccessNo) AS GroupAccountNo,
			CC.AccessNo AS AccountNo,
			CC.Product AS Product,
			SUM(InFundsAmount) AS EOD_Balance,
			SUM(InFundsAmount) AS EOD_CCYBalance,
			'NZD' AS CCY,
			1.0 AS FXSpotFactor,
			0 AS SumofEODLimit
FROM		MRU_Retail_Card_InFunds CC
			LEFT JOIN ClientGrouping CG ON CC.AccessNo = CG.AccountNo
GROUP BY	CC.EODDate,
			ISNULL(CG.GroupAccountNo, CC.AccessNo), 
			CC.AccessNo,
			CC.Product
HAVING		SUM(InFundsAmount) > 0

UNION ALL

-- LFT (NZD Loaded for Travel)
SELECT	LFT.EodDate AS EodDate, 
		'1047' AS GroupAccountNo, 
		'1047' AS AccountNo, 
		'S76' AS Product, 
		LFT.EodBalance AS EOD_Balance, 
		LFT.EodBalance AS EOD_CCYBalance, 
		'NZD' AS CCY, 
		1.0 AS FXSpotFactor, 
		0 AS SumOfEODLimit
FROM	(
		Select	EodDate, SUM(EodBalance) AS EodBalance
		From	Ultracs.AccountsEOD
		Where	(AccountNo = '1046' AND Product = 'S76.5') OR (AccountNo = '1046' AND Product = 'S76.9')
				OR (AccountNo = '1047' AND Product = 'S76.1') OR (AccountNo = '1047' AND Product = 'S76.5') OR (AccountNo = '1047' AND Product = 'S76.6') OR (AccountNo = '1047' AND Product = 'S76.7')
				OR (AccountNo = '1048' AND Product = 'S76.2') OR (AccountNo = '1048' AND Product = 'S76.5') OR (AccountNo = '1048' AND Product = 'S76.6')
				OR (AccountNo = '1050' AND Product = 'S76.7')
				OR (AccountNo = '1074' AND Product = 'S76') OR (AccountNo = '1074' AND Product = 'S76.1') OR (AccountNo = '1074' AND Product = 'S76.2') OR (AccountNo = '1074' AND Product = 'S76.3') OR (AccountNo = '1074' AND Product = 'S76.4') OR (AccountNo = '1074' AND Product = 'S76.5') OR (AccountNo = '1074' AND Product = 'S76.6') OR (AccountNo = '1074' AND Product = 'S76.7') 
				OR (AccountNo = '1075' AND Product = 'S76')
		Group By EodDate
		) LFT;

DELETE FROM MRU_Liquidity_Funding_Balances
WHERE AccountNo = '2345525' AND (Product = 'S30' OR Product = 'S30.1' OR Product = 'S30.2' OR Product = 'S30.3');

END

GO
/****** Object:  StoredProcedure [dbo].[MRU_Liquidity_Funding_Balances_Retail_TD_MakeTable]    Script Date: 2/11/2023 1:33:40 pm ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


CREATE PROCEDURE [dbo].[MRU_Liquidity_Funding_Balances_Retail_TD_MakeTable]	
AS
BEGIN
SET NOCOUNT ON;

/********************************************************************************************************
 * This populates an interim table MRU_Liquidity_Funding_Balances_Retail_TD which consists of contractual and breakable amount.
 * The eligibility of early withdrawal is implemented in RetailTermDepositFull.
 *
 ********************************************************************************************************
 *
 * @version 1.0
 * @author	Stephen Chin
 * @date	2020.08.13
 * 
 * - Start version control
 *
*/

TRUNCATE TABLE MRU_Liquidity_Funding_Balances_Retail_TD;

DECLARE @reportDate datetime; 
SET		@reportDate = (Select ReportEndDate From Variables);

INSERT MRU_Liquidity_Funding_Balances_Retail_TD

-- Contractual
SELECT	TD.FileCreateDate AS FileCreateDate, 
		TD.Accounts AS Accounts, 
		TD.Product AS Product, 
		TD.LodegementDate AS LodegementDate, 
		TD.MaturityDate AS MaturityDate, 
		TD.IntFreq AS IntFreq, 
		TD.Term AS Term, 
		TD.Rate AS Rate, 
		CASE WHEN TD.BreakableFlag = 1 THEN TD.NewAmount ELSE TD.NewAmount + TD.NewBreakable END AS Amount, 
		TD.TDNo AS TDNo,
		'N' AS BreakFlag
FROM	RetailTermDepositFull TD
WHERE	TD.FileCreateDate = @reportDate

UNION ALL

-- Breakable at-risk
SELECT	TD.FileCreateDate AS FileCreateDate, 
		TD.Accounts AS Accounts, 
		TD.Product AS Products, 
		(Select ReportEndDate From Variables) AS LodegementDate, 
		(Select ReportEndDate From Variables) AS MaturityDate, 
		NULL AS IntFreq, 
		NULL AS Term, 
		NULL AS Rate, 
		TD.NewBreakable AS Amount, 
		TD.TDNo AS TDNo,
		'Y' AS BreakFlag 
FROM	RetailTermDepositFull TD
WHERE	TD.FileCreateDate = @reportDate
		AND TD.BreakableFlag = 1;
END

GO
/****** Object:  StoredProcedure [dbo].[MRU_Liquidity_RBNZ_DailyReporting_MakeTable]    Script Date: 2/11/2023 1:33:40 pm ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO



CREATE PROCEDURE [dbo].[MRU_Liquidity_RBNZ_DailyReporting_MakeTable]
AS
BEGIN	
	SET NOCOUNT ON;

/********************************************************************************************************
 * This output table stores summary of funding profile for regulatory calculation.
 *
 ********************************************************************************************************
 *
 * >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
 * @version 1.0
 * @author	Stephen Chin
 * @date	2020.08.13
 * 
 * - Start version control
 *
 * @version 1.1
 * @author	Stephen Chin
 * @date	2021.04.28
 * - Append sum of face value, book value and market value fields
 *
 * @version 1.2: SQL model change 1.11 - Mapping face value to Funding value & adding Carrying Value and CCYCarrying Value 
 * @author	My Phan
 * @date	2022.10.05
 * - adding Carrying value for Funding value
 *		SUM(CarryingValue) AS CarryingValue
 *
 *
*/

Declare @reportDate date
Set		@reportDate = (Select CONVERT(DATE, Variable, 103) From tblVariables Where No = 1);

DELETE FROM MRU_RBNZ_Funding_Balances_1;

INSERT		MRU_RBNZ_Funding_Balances_1

SELECT		Category, 
			FundingType, 
			CASE WHEN ProductGroup = 'MMCSH' THEN
				'MMF'
			ELSE
				CASE WHEN ProductGroup = 'FXFUND' THEN
					'FXF'
				ELSE
					CASE WHEN ProductGroup = 'MTNFLT' THEN
						'MTN'
					ELSE
						CASE WHEN ProductGroup = 'SENFLT' THEN
							'SEN'
						ELSE
							CASE WHEN ProductGroup = 'SUBFLT' THEN 
								'SUB'
							ELSE
								ProductGroup
							END
						END
					END
				END
			END AS ProductGrouping, 
			ProductType, 
			Sector, 
			Banding, 
			dbo.GetTimeProfile(@reportDate, RemaingTermDays) AS TimeProfile,
			dbo.GetBucketProfileFunding(dbo.GetTimeProfile(@reportDate, RemaingTermDays)) AS BucketProfile,
			SUM(Balance) AS [Funding Balance], 
			EodDate, 
			ClientType,
			SUM(FaceValue) AS FaceValue, 
            SUM(BookValue) AS BookValue, 
            SUM(MarketValue) AS MarketValue,
			--v1.2 new column
			SUM(CarryingValue) AS CarryingValue

FROM		AllData

GROUP BY	Category, 
			FundingType, 
			CASE WHEN ProductGroup = 'MMCSH' THEN
				'MMF'
			ELSE
				CASE WHEN ProductGroup = 'FXFUND' THEN
					'FXF'
				ELSE
					CASE WHEN ProductGroup = 'MTNFLT' THEN
						'MTN'
					ELSE
						CASE WHEN ProductGroup = 'SENFLT' THEN
							'SEN'
						ELSE
							CASE WHEN ProductGroup = 'SUBFLT' THEN 
								'SUB'
							ELSE
								ProductGroup
							END
						END
					END
				END
			END, 
			ProductType, 
			Sector, 
			Banding, 
			dbo.GetTimeProfile(@reportDate, RemaingTermDays), 
			dbo.GetBucketProfileFunding(dbo.GetTimeProfile(@reportDate, RemaingTermDays)), 
			EodDate, 
			ClientType
	
ORDER BY	Category, 
			FundingType, 
			CASE WHEN ProductGroup = 'MMCSH' THEN
				'MMF'
			ELSE
				CASE WHEN ProductGroup = 'FXFUND' THEN
					'FXF'
				ELSE
					CASE WHEN ProductGroup = 'MTNFLT' THEN
						'MTN'
					ELSE
						CASE WHEN ProductGroup = 'SENFLT' THEN
							'SEN'
						ELSE
							CASE WHEN ProductGroup = 'SUBFLT' THEN 
								'SUB'
							ELSE
								ProductGroup
							END
						END
					END
				END
			END, 
			Banding, 
			ClientType;

END



GO
/****** Object:  StoredProcedure [dbo].[MRU_Liquidity_RBNZ_PrivateReporting_MakeTable]    Script Date: 2/11/2023 1:33:40 pm ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


CREATE PROCEDURE [dbo].[MRU_Liquidity_RBNZ_PrivateReporting_MakeTable]
       
AS
BEGIN
	SET NOCOUNT ON;

/********************************************************************************************************
 * This output table stores summary of funding profile for monthly RBNZ template.
 *
 ********************************************************************************************************
 *
 * @version 1.0
 * @author	Stephen Chin
 * @date	2020.08.13
 * - Start version control
 * - Add new time bucket code of '50Y' to RBNZInitialMaturityBucket
 * 
 * @version 1.1: SQL model change 1.11 - Mapping face value to Funding value & adding Carrying Value and CCYCarrying Value 
 * @author	My Phan
 * @date	2022.10.05
 * - Add Carrying value column
			SUM(AD.CarryingValue) AS SumOfCarryingValue
*/

Declare		@reportDate date
Set			@reportDate = (Select CONVERT(DATE, Variable, 103) From tblVariables Where No = 1);

DELETE FROM MRU_RBNZ_Funding_Balances_2;

INSERT		MRU_RBNZ_Funding_Balances_2

SELECT		AD.EodDate, 
            AD.FundingType, 
            AD.ProductGroup, 
            AD.Banding, 
            AD.Residency, 
            CASE WHEN CCY = 'NZD' THEN 'NZD' ELSE 'Non-NZD' END AS [CCY Category], 
            CASE WHEN CCY IN ('AUD', 'CAD', 'CHF', 'EUR', 'GBP', 'JPY', 'USD', 'NZD') THEN CCY ELSE 'Other' END AS [CCY Detail], 
            PM.RBNZProductMapping1, 
            PM.RBNZProductMapping2, 
            dbo.GetBucketProfileFundingRBNZ(dbo.GetTimeProfile(@reportDate, AD.RemaingTermDays)) AS BucketProfileFunding,
            CASE WHEN AD.LodegementDate IS NOT NULL THEN
                    CASE WHEN AD.OriginalTermDays IS NOT NULL THEN
                            CASE WHEN dbo.GetTimeProfile(LodegementDate, DATEDIFF(DD, LodegementDate, ISNULL(MaturityDate, @reportDate))) IN ('3Y', '4Y', '5Y', '7Y', '10Y', '15Y', '20Y', '50Y') THEN 
                                'Over 2 Years'
                            ELSE 
                                'Out to 2 Years'
                            END
                    ELSE
                            'Out to 2 Years'
                    END
            ELSE
                    CASE WHEN AD.OriginalTermDays IS NOT NULL THEN
                            CASE WHEN dbo.GetTimeProfile(@reportDate, OriginalTermDays) IN ('3Y', '4Y', '5Y', '7Y', '10Y', '15Y', '20Y', '50Y') THEN 
                                'Over 2 Years'
                            ELSE 
                                'Out to 2 Years'
                            END
                    ELSE
                            'Out to 2 Years'
                    END
            END AS RBNZInitialMaturityBucket,
            SUM(AD.Balance) AS SumOfBalance, 
            AD.ClientType, 
            SUM(AD.FaceValue) AS SumOfFaceValue, 
            SUM(AD.BookValue) AS SumOfBookValue, 
            SUM(AD.MarketValue) AS SumOfMarketValue,
			-- v1.2 add new column
			SUM(AD.CarryingValue) AS SumOfCarryingValue
       
FROM		AllData AD
            LEFT JOIN MRU_Product_Mapping PM ON AD.ProductGroup = PM.ProductGrp
       
GROUP BY	AD.EodDate, AD.FundingType, AD.ProductGroup, AD.Banding, AD.Residency, 
            CASE WHEN CCY = 'NZD' THEN 'NZD' ELSE 'Non-NZD' END, 
            CASE WHEN CCY IN ('AUD', 'CAD', 'CHF', 'EUR', 'GBP', 'JPY', 'USD', 'NZD') THEN CCY ELSE 'Other' END, 
            PM.RBNZProductMapping1, 
            PM.RBNZProductMapping2, 
            dbo.GetBucketProfileFundingRBNZ(dbo.GetTimeProfile(@reportDate, AD.RemaingTermDays)), 
            CASE WHEN AD.LodegementDate IS NOT NULL THEN
                    CASE WHEN AD.OriginalTermDays IS NOT NULL THEN
                            CASE WHEN dbo.GetTimeProfile(LodegementDate, DATEDIFF(DD, LodegementDate, ISNULL(MaturityDate, @reportDate))) IN ('3Y', '4Y', '5Y', '7Y', '10Y', '15Y', '20Y', '50Y') THEN 
                                'Over 2 Years'
                            ELSE 
                                'Out to 2 Years'
                            END
                    ELSE
                            'Out to 2 Years'
                    END
            ELSE
                    CASE WHEN AD.OriginalTermDays IS NOT NULL THEN
                            CASE WHEN dbo.GetTimeProfile(@reportDate, OriginalTermDays) IN ('3Y', '4Y', '5Y', '7Y', '10Y', '15Y', '20Y', '50Y') THEN 
                                'Over 2 Years'
                            ELSE 
                                'Out to 2 Years'
                            END
                    ELSE
                            'Out to 2 Years'
                    END
            END,
            AD.ClientType
       
ORDER BY    AD.ClientType;

END

GO
/****** Object:  StoredProcedure [dbo].[MRU_LiquidityHistoryData_BD_Upload]    Script Date: 2/11/2023 1:33:40 pm ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[MRU_LiquidityHistoryData_BD_Upload]
AS
BEGIN
	
	SET NOCOUNT ON;

	/********************************************************************************************************
	*	PROCEDURE PURPOSE:                                                                                  *
	*	------------------                                                                                  *
	*                                                                                                       *
	*	Transform VBA that downloads Liquidity Cost Data in spreadsheet	and upload to LiquidityHistory DB   *
	*	To SQL stored Procedures enhancing performance and accommodating stakeholder's requirement          *
	*                                                                                                       *
	*
	********************************************************************************************************
	*
	* @version 1.17 - System Change SQL  – Schedule Flash Report
	* @author	William Hsiao
	* @date	2023.07.31
	* 
	* Replaced Manually run queries in Excel by creating procedures to be scheduled
	* 
	********************************************************************************************************

	*/
	
	DECLARE @dEodDate			DATETIME
	SET @dEodDate = (
				SELECT TOP	1 
							DATEFROMPARTS(RIGHT(Variable,4), SUBSTRING (Variable, 4,2), LEFT(Variable, 2))
					FROM	tblVariables 
					WHERE	Description = 'ReportEndDate' 
					ORDER BY 1 DESC
					);

	DELETE 
		FROM	LiquidityHistory.dbo.LiquidityCost_Data 
		WHERE	EodDate = @dEodDate;

	/* ======================================================================================================
	**  
	** Upload Liquidity Cost Data to Liquidity History DB:
	** Liquidity Cost Data is used to calculate LCA, one of the Fund transfer pricing (FTP) compoenets
	** Noting: Notice Saver (S8) applies different method to calculate Liquidity Cost.
	**
	** ======================================================================================================
	*/

	-- sqlQueryCustomer 
	--

	INSERT 
		INTO	LiquidityHistory.dbo.LiquidityCost_Data 
		SELECT  EodDate, FundingType, ProductGroup, Banding, OriginalTermDays, SUM(Balance) AS TotalBalance  
                       FROM AllData
                       WHERE (Source = 'Retail' AND ProductGroup <> 'S8') OR (ProductGroup IN ('FXFUND','FCA')) 
                       OR (GroupAccountNo not in (SELECT GroupAccountNo FROM MRU_TMS_ClientGroup_Mapping WHERE ClientType = 'INTERNAL_P') 
                       AND ProductGroup in ('S61','I20')) 
                       GROUP BY EodDate, FundingType, ProductGroup, Banding, OriginalTermDays 
                       ORDER BY 1,2,3;

	-- (Non-called) sqlQueryNoticeSaver 
	-- 

	INSERT 
		INTO	LiquidityHistory.dbo.LiquidityCost_Data 
		SELECT  EodDate, FundingType, ProductGroup, Banding, OriginalTermDays, SUM(Balance) AS TotalBalance  
                          FROM AllData 
                          WHERE ProductGroup = 'S8' 
                          GROUP BY EodDate, FundingType, ProductGroup, Banding, OriginalTermDays 
                          ORDER BY 1,2,3;

	-- sqlQueryTreasury 
	--
	
	INSERT 
		INTO	LiquidityHistory.dbo.LiquidityCost_Data 
		SELECT  EodDate, FundingType, ProductGroup, Banding, OriginalTermDays, SUM(Balance) AS TotalBalance  
                       FROM AllData
                       WHERE ProductGroup in ('COV', 'COVFLT', 'ECP', 'MTN', 'MTNFLT', 'RCD', 'SEN', 'SUB', 'EWTD') 
                       GROUP BY EodDate, FundingType, ProductGroup, Banding, OriginalTermDays 
                       ORDER BY 1,2,3;


	-- (Called) sqlQueryNoticeSaver 
	--

	DELETE 
		FROM	LiquidityHistory.dbo.LiquidityNoticeSaver_Data 
		WHERE	EodDate = @dEodDate;

	INSERT
		INTO	LiquidityHistory.dbo.LiquidityNoticeSaver_Data 
	    SELECT  EodDate, FundingType, ProductGroup, Banding, RemaingTermDays, SUM(Balance) AS TotalBalance 
                        FROM AllData 
                        WHERE ProductGroup = 'S8' 
                        GROUP BY EodDate, FundingType, ProductGroup, Banding, RemaingTermDays 
                        ORDER BY 1,2,3,4,5


		/* ======================================================================================================
		**  
		** Upload Liquidity Size Band to Liquidity History DB:
		** Size banding is used to allocate the banks Liquidity Cost Adjustment (LCA) for deposits 
		**   And determine which Stable Funding Premium (SFP) curve a term deposit is worth.
		** 
		** ======================================================================================================
		*/	
	
	DELETE 
		FROM	LiquidityHistory.dbo.LiquiditySizeBand_Data 
		WHERE	EodDate = @dEodDate;

	INSERT 
		INTO	LiquidityHistory.dbo.LiquiditySizeBand_Data 
		SELECT DISTINCT EodDate, AccountNo, 
                    CASE WHEN ClientType = 'FI' THEN 'MarketFunding' ELSE Banding END AS SizeBand, 
                    clienttotalbalance 
                    FROM AllData 
                    WHERE Banding  NOT LIKE '0-5%' OR  ClientType = 'FI' 
                    ORDER BY 2;

END;



GO
/****** Object:  StoredProcedure [dbo].[MRU_LiquidityHistoryData_NonBD_Replicate]    Script Date: 2/11/2023 1:33:40 pm ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[MRU_LiquidityHistoryData_NonBD_Replicate]
AS
BEGIN
	
	SET NOCOUNT ON;



	/********************************************************************************************************
	*	PROCEDURE PURPOSE:                                                                                  *
	*	------------------                                                                                  *
	*                                                                                                       *
	*	Replicate the last Business day Liquidity Data for Non Business day                                 *
	*	As Liquidity reports only required to run in working day whereas FTP demands 7 days information     *
	*																									    *
	*   Non Business day: Public holidays or Weekends													    *	
	*																										*
	********************************************************************************************************
	*
	* @version 1.17 - System Change SQL  – Schedule Flash Report
	* @author	William Hsiao
	* @date	2023.07.31
	* 
	* Replaced Manually run queries in Excel by creating procedures to be scheduled
	* 
	********************************************************************************************************
	*/

	DECLARE @dNonBDDate			DATETIME
	DECLARE @dBDDate			DATETIME

	SET @dNonBDDate = (SELECT DATEADD(day, -1, CAST(GETDATE() AS date)));
	
	-- LiquidityCost_Data Table
	--	

	SET @dBDDate = (SELECT MAX(EODDate) from LiquidityHistory.dbo.LiquidityCost_Data);

	WHILE (@dBDDate<@dNonBDDate)
	BEGIN
		
		DELETE 
			FROM	LiquidityHistory.dbo.LiquidityCost_Data 
			WHERE	EodDate = @dBDDate+1;

		INSERT 
			INTO	LiquidityHistory.dbo.LiquidityCost_Data 
			SELECT  @dBDDate+1, FundingType, ProductGroup, Banding, OriginalTermDays, TotalBalance
			FROM	LiquidityHistory.dbo.LiquidityCost_Data 
			WHERE   EodDate = @dBDDate

		SET @dBDDate = @dBDDate +1;

	END

	-- LiquidityNoticeSaver Table
	--	

	SET @dBDDate = (SELECT MAX(EODDate) from LiquidityHistory.dbo.LiquidityNoticeSaver_Data);
	
	WHILE (@dBDDate<@dNonBDDate)
	BEGIN
		
		DELETE 
			FROM	LiquidityHistory.dbo.LiquidityNoticeSaver_Data 
			WHERE	EodDate = @dBDDate+1;

		INSERT 
			INTO	LiquidityHistory.dbo.LiquidityNoticeSaver_Data 
			SELECT  @dBDDate+1, FundingType, ProductGroup, Banding, RemainingTermDays, TotalBalance
			FROM	LiquidityHistory.dbo.LiquidityNoticeSaver_Data 
			WHERE   EodDate = @dBDDate

		SET @dBDDate = @dBDDate +1;

	END

	-- LiquiditySizeBand Table
	--	

	SET @dBDDate = (SELECT MAX(EODDate) from LiquidityHistory.dbo.LiquiditySizeBand_Data);

	WHILE (@dBDDate<@dNonBDDate)
	BEGIN
		
		DELETE 
			FROM	LiquidityHistory.dbo.LiquiditySizeBand_Data 
			WHERE	EodDate = @dBDDate+1;

		INSERT 
			INTO	LiquidityHistory.dbo.LiquiditySizeBand_Data 
			SELECT  @dBDDate+1, AccessNo, Banding, CustomerBalance
			FROM	LiquidityHistory.dbo.LiquiditySizeBand_Data 
			WHERE   EodDate = @dBDDate

		SET @dBDDate = @dBDDate +1;

	END;


END;
GO
/****** Object:  StoredProcedure [dbo].[MRU_Pipeline_Business_MakeTable]    Script Date: 2/11/2023 1:33:40 pm ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[MRU_Pipeline_Business_MakeTable]	
AS
BEGIN	

	SET NOCOUNT ON; 

	/********************************************************************************************************
	*	PROCEDURE PURPOSE: Details as at version 1                                                          *
	*	------------------                                                                                  *
	*                                                                                                       *
    *	This generates one interim table MRU_Pipeline_Business_Data and two output tables                   *
    *	MRU_Pipeline_Business_Detail and MRU_Pipeline_Business_Summary.                                     *
	*                                                                                                       *
	*	The Commitment_Type field is populated using function GetCommitmentType with inputs of				*
	*	Position_Date, Date_Loan_Required and Tolerance.													*
	*                                                                                                       *
	*	The function returns string which describes type of commitment based on the number of days			*
	*	between Position_Date and Date_Loan_Required. The tolerance is set to 7 here which lets the			*
	*	GetCommitmentType returns 'OLD COMMITMENT' if Date_Loan_Required is 7 days or older than			*
	*	Position_Date.																						*
	*                                                                                                       *
	*	Application_Type 'Q', 'S' and 'T' represents "Cheque book application", "Security/Entity swap"		*
	*	and "Prospect", respectively.																		*
	*                                                                                                       *
	*	Workflow_Status 'A' and 'P' represents "Document tracking" and "Document preparation" stage,		*
	*	respectively.																						*
	*		These are included only if Workflow_Date is less than 91 days.									*
	*		If Workflow_Status is 'T' (pre settlement) then all cases are included regardless of			*
	*	    Workflow_Date.																					*
	*                                                                                                       *
	*	Workflow_Status 'ABB:E' and 'ABB:S' in CRE_Commitment represents finalised and post settlement,		*
	*	respectively.Hence, the undrawn portion of progressive drawdown is always treated as OLD			*
	*	COMMITMENT, i.e., Date_Loan_Required is older than 7 days of tolerance.								*
	*                                                                                                       *
	*********************************************************************************************************
	*                                                                                                       *
	* @version	:	1.0                                                                                     *
	* @author	:	Stephen Chin                                                                            *
	* @date		:	2020.08.13                                                                              *
	* @details	:   Start version control                                                                   * 
	*                                                                                                       *
	*********************************************************************************************************
	*                                                                                                       *
	* @version	:	2.0                                                                                     *
	* @location :   S:\dept\Finance\Market Risk and Wholesale Accounting\Model\01_Liquidity\SQL\            *
	* 	            1.19 Undrawn Limit, Pipeline Aggregation, Joint Account\Final SQL - sent to IT (v1.19)  *
	* @author	:	Shaun Paul Barnarde                                                                     *
	* @date		:	2020.10.16                                                                              *
	* @details	:                                                                                           * 
	*                                                                                                       *
	* 1.) V1.0 definiion is that aggregation occurs by application#, and does not consider product          *
	*      a.) In case where multiple products are associated with the same application#, for               *
	*          example, a reduction in limit of a product, this would impact the aggregated                 *
	*          commitment to a customer.                                                                    *
	*		b.) As, well, to cater for multiple transactions, for an application and product pair,          *
	*          aggregation should be performed by product.                                                  *
	*                                                                                                       *
	* 2.) Therefore, V2.0 therefore defines commitments as aggregated by application# as well as            *
	*     Product.                                                                                          *
	*                                                                                                       *
	* 3.) Additionally, providing visibility to excel, the product code is included into the summary table. *
	*                                                                                                       *
	*     ALTER TABLE MRU_Pipeline_Business_Detail ADD Product_Code varchar(8) NULL;                        *
	*                                                                                                       *
	*********************************************************************************************************
	*/

	DECLARE @tolerance		INT;
	DECLARE	@reportDate		Date;
	SET		@tolerance = 7;
	SET		@reportDate = (Select ReportEndDate From Variables);

	-- prepare table
	--

	DELETE MRU_Pipeline_Business_Data;
	DELETE MRU_Pipeline_Business_Detail;
	DELETE MRU_Pipeline_Business_Summary;

	-- ------------------------------------------------------------------------------------------------------
	--                                                                                                     --
	-- STEP 1: Populate Data Table:                                                                        --
	--                                                                                                     --
	-- Is used as a record of data returned by query                                                       --
	--                                                                                                     --
	-- ------------------------------------------------------------------------------------------------------

	INSERT INTO	MRU_Pipeline_Business_Data

		(		Position_Date,						Application_Number,					Application_Date,	
				Business_Loan_Application_Type,		Workflow_Status,					Workflow_Date, 
				Portion_Number,						Portion_Type,						Repayment_Frequency, 
				Date_Loan_Required,					Loan_Purpose,						Product_Code, 
				Fixed_Period,						Total_Rate_Pcent,					Proposed_Limit, 
				Current_Limit,						Limit_Change,						Settlement_Date, 
				Current_Balance
				)
 
	SELECT		@reportDate,
				AA.Application_Number,
				AA.Application_Date,
				AA.Business_Loan_Application_Type,
				AA.Workflow_Status,
				AA.Workflow_Date,
				ALP.Portion_Number,
				ALP.Portion_Type,
				ALP.Repayment_Frequency,
				ALP.Date_Loan_Required,
				ALP.Loan_Purpose,
				ALP.Product_Code,
				ALP.Fixed_Period,
				ALP.Total_Rate_Pcent,
				ALP.Proposed_Limit,
				ALP.Current_Limit,
				ALP.Limit_change,
				ALP.Settlement_Date,
				ALP.Current_Balance 
		
		FROM	ActivateBB.pipeline_Activate_Applications AS AA 
				INNER JOIN ActivateBB.pipeline_Activate_Loan_Portions AS ALP 
						ON AA.Application_Number = ALP.Application_Number
		
		WHERE	AA.Business_Loan_Application_Type NOT IN ('Q','S','T')
			AND (AA.Final_Decision = 'A') 
			AND ALP.Settlement_Date IS NULL
			AND ((AA.Workflow_Status IN ('A','P') AND AA.Workflow_Date > DATEADD(MM, -3, @reportDate)) OR (AA.Workflow_Status IN ('T'))) 
			AND (ALP.Product_Code LIKE 'L1%' OR ALP.Product_Code LIKE 'L30%');

	-- ------------------------------------------------------------------------------------------------------
	--                                                                                                     --
	-- STEP 2 : Populate Detail Table:                                                                     --    
	--                                                                                                     --
	-- Preprocesses data, aggregating by application#, data loan required and product                      --
	--                                                                                                     --
	-- ------------------------------------------------------------------------------------------------------

	INSERT  MRU_Pipeline_Business_Detail (	
	
				Position_Date,						Application_Number,					Workflow_Status, 
				Date_Loan_Required,					Limit_Change,						Commitment_Type,
			
				-- v2.0
				-- Extend table to include Product_Code
				--
				Product_Code
			
				)

			SELECT		Position_Date				AS Position_Date,
						Application_Number			AS Application_Number,
						Workflow_Status				AS Workflow_Status,
						Date_Loan_Required			AS Date_Loan_Required,
						ROUND(SUM(Limit_Change), 2) AS Limit_Change,

						dbo.GetCommitmentType(Position_Date, Date_Loan_Required, @tolerance) 
													AS Commitment_Type,

						-- v2.-
						-- Extend select to include Product_Code, and making this visible to excel
						--
						Product_Code

				FROM	MRU_Pipeline_Business_Data
				
				GROUP BY 
						Position_Date, Application_Number, Workflow_Status, Date_Loan_Required,
					
						-- v2.0
						-- extend the grouping clause to include, Product_Code
						Product_Code

			HAVING		SUM(Limit_Change) > 0

	UNION ALL

			SELECT		@reportDate					AS Position_Date,
						Application_Number			AS Application_Number,
						RIGHT(Workflow_Status, 1)	AS Workflow_Status,
						Date_Loan_Required			AS Date_Loan_Required, 
						ROUND(SUM(Exp_Undrawn), 2)	AS Limit_Change, 
					
						-- v2.0, 
						-- correction as these are not all old commitment
						--'OLD COMMITMENT' AS Commitment_Type,
						--
						dbo.GetCommitmentType(@reportDate, Date_Loan_Required, @tolerance) AS Commitment_Type,

						-- v2.0
						-- extend select to include Product_Code, making this visible to excel
						--
						Loan_Product

				FROM	CRE_Commitment
				
				WHERE	Facility_Type IN ('LN')
					AND Workflow_Status IN ('ABB:E', 'ABB:S')
				
				GROUP BY	
						Application_Number, RIGHT(Workflow_Status, 1), Date_Loan_Required,

						-- v2.0
						--
						Loan_Product;

	-- ------------------------------------------------------------------------------------------------------
	--                                                                                                     --
	-- Populate Summary Table:                                                                             --
	--                                                                                                     --
	-- Consume from detail table, and aggregating by commitment type, and scale each period by relevant    --
	-- regulatory percentage.                                                                              -- 
	--                                                                                                     --
	-- position_date| commitment_type| pipeline_amt| 1w_weight| 1w_pipeline_amt| 1m_weight| 1m_pipeline_amt--
	-- <date>       | old commitment | <amt>       |  15%     | % * <amt>      |  15%     | % * <amt>      --
	-- <date>       | past due currnt| <amt>       | 100%     | % * <amt>      | 100%     | % * <amt>      --
	-- <date>       | due within 1w  | <amt>       | 100%     | % * <amt>      | 100%     | % * <amt>      --
	-- <date>       | between 1w & 1m| <amt>       |  15%     | % * <amt>      | 100%     | % * <amt>      --
	-- <date>       | long commitment| <amt>       |  15%     | % * <amt>      |  15%     | % * <amt>      --
	--                                                                                                     --
	-- ------------------------------------------------------------------------------------------------------

	INSERT  MRU_Pipeline_Business_Summary (
	
				Position_date,				Commitment_Type,							PipeLine_Amount, 
				[1Week_Weighting],			[1Week_pipeline_Amount], 
				[1Month_Weighting],			[1Month_Pipeline_Amount]
				
				)

			SELECT		Position_Date,
						Commitment_Type,
						ROUND(SUM(Limit_Change), 2),
						CASE 
							WHEN Commitment_Type IN ('DUE WITHIN 1 WEEK', 'PAST DUE CURRENT') 
							THEN '100%' 
							ELSE '15%' 
							END,
						
						CASE 
							WHEN Commitment_Type IN ('DUE WITHIN 1 WEEK', 'PAST DUE CURRENT') 
							THEN ROUND(SUM(Limit_Change), 2) 
							ELSE ROUND(SUM(Limit_Change) * 15/100.0, 2) 
							END,
						
						CASE 
							WHEN Commitment_Type IN ('DUE WITHIN 1 WEEK', 'PAST DUE CURRENT', 'BETWEEN 1 WEEK AND A MONTH') 
							THEN '100%' 
							ELSE '15%' 
							END,

						CASE 
							WHEN Commitment_Type IN ('DUE WITHIN 1 WEEK', 'PAST DUE CURRENT', 'BETWEEN 1 WEEK AND A MONTH') 
							THEN ROUND(SUM(Limit_Change), 2) 
							ELSE ROUND(SUM(Limit_Change) * 15/100.0, 2) 
							END

				FROM	MRU_Pipeline_Business_Detail
				
				GROUP BY
						Position_Date, Commitment_Type;

END

GO
/****** Object:  StoredProcedure [dbo].[MRU_Pipeline_Retail_MakeTable]    Script Date: 2/11/2023 1:33:40 pm ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[MRU_Pipeline_Retail_MakeTable]	
AS
BEGIN	
SET NOCOUNT ON; 

/********************************************************************************************************
 * This generates one interim table MRU_Pipeline_Retail_Data and two output tables MRU_Pipeline_Retail_Detail and MRU_Pipeline_Retail_Summary.
 * 
 * The Commitment_Type field is populated using function GetCommitmentType with inputs of Position_Date, Date_Loan_Required and Tolerance. 
 * The function returns string which describes type of commitment based on the number of days between Position_Date and Date_Loan_Required.
 * The tolerance is set to 7 here which lets the GetCommitmentType returns 'OLD COMMITMENT' if Date_Loan_Required is 7 days or older than Position_Date.
 * 
 * Application_Type 'P' and 'H' represents "Pre-qualified" and "Home loan application", respectively.
 * 
 * Workflow_Status 'A' and 'O' represents "Fast queue" and "Documentation - letter of offer sent to the customer" stage, respectively. 
 * These are included only if Workflow_Date is less than 91 days.
 * If Workflow_Status is 'T' (Pending settlement) then all cases are included regardless of Workflow_Date.
 *
 ********************************************************************************************************
 *
 * @version 1.0
 * @author	Stephen Chin
 * @date	2020.08.13
 *
 * - Start version control
 *
 *
*/

DECLARE @tolerance INT;
DECLARE	@reportDate Date;
SET		@tolerance = 7;
SET		@reportDate = (Select ReportEndDate From Variables);

TRUNCATE TABLE MRU_Pipeline_Retail_Data
TRUNCATE TABLE MRU_Pipeline_Retail_Detail
TRUNCATE TABLE MRU_Pipeline_Retail_Summary

-- Populate Data Table
INSERT INTO MRU_Pipeline_Retail_Data
			(Position_Date, Entity_Code, Application_Number, Access_Number, Application_Type, Workflow_Status,
			Workflow_Date, New_Lending_Amount, Component_Number, Portion_Amount, Interest_Rate, New_Interest_Rate,
			Portion_Type, Fixed_Period, Application_Date, Date_Loan_Required, Settlement_Date, Loan_Purpose)

SELECT		@reportDate,
			AA.Entity_Code,
			AA.Application_Number,
			AA.Access_Number,
			AA.Application_Type,
			AA.Workflow_Status,
			AA.Workflow_Date,
			AA.New_Lending_Amount,
			ALP.Component_Number,
			ALP.Portion_Amount,
			ALP.Interest_Rate,
			ALP.New_Interest_Rate,
			ALP.Portion,
			ALP.Fixed_Period,
			AA.Application_Date,
			AA.Date_Loan_Required,
			AA.Settlement_Date,
			AA.Loan_Purpose	
FROM		ActivateKB.pipeline_vwActivateApplicationsSummary AS AA 
			INNER JOIN ActivateKB.pipeline_Activate_Loan_Components AS ALP ON AA.Application_Number = ALP.Application_Number
WHERE		AA.Application_Type IN ('P', 'H')
			AND AA.Settlement_Date IS NULL
			AND ((AA.Workflow_Status IN ('A', 'O') AND AA.Workflow_Date > DATEADD(MM, -3, @reportDate)) OR (AA.Workflow_Status IN ('T'))) 
			AND ISNULL(ALP.Portion_Amount,0) > 0;


-- Populate Detail Table
INSERT INTO	MRU_Pipeline_Retail_Detail
			(Position_Date, Application_Number, Workflow_Status, Date_Loan_Required, Portion_Amount, Commitment_Type)

SELECT		Position_Date AS Position_Date,
			Application_Number AS Application_Number,
			Workflow_Status AS Workflow_Status,
			Date_Loan_required AS Date_Loan_Required,
			ROUND(SUM(Portion_Amount),2) AS Portion_Amount,
			CASE WHEN Workflow_Status IN ('T') THEN 
				dbo.GetCommitmentType(Position_Date, Date_Loan_Required, @tolerance)
			ELSE
				CASE WHEN Workflow_Status IN ('A', 'O') THEN
					'OLD COMMITMENT'
				END
			END AS Commitment_Type
FROM		MRU_Pipeline_Retail_Data
GROUP BY	Position_Date, Application_Number, Workflow_Status, Date_Loan_Required 
HAVING		SUM(Portion_Amount) > 0
ORDER BY	Position_Date, Application_Number, Workflow_Status, Date_Loan_Required;


-- Populate Summary Table
INSERT INTO	MRU_Pipeline_Retail_Summary
			(Position_date, Commitment_Type, PipeLine_Amount, [1Week_Weighting], [1Week_pipeline_Amount], [1Month_Weighting], [1Month_Pipeline_Amount])
	
SELECT		Position_Date,
			Commitment_Type,
			ROUND(SUM(Portion_Amount), 2),
			(CASE WHEN Commitment_Type IN ('DUE WITHIN 1 WEEK', 'PAST DUE CURRENT')	THEN '100%' ELSE '15%' END),
			(CASE WHEN Commitment_Type IN ('DUE WITHIN 1 WEEK', 'PAST DUE CURRENT') THEN ROUND(SUM(Portion_Amount), 2) ELSE ROUND(SUM(Portion_Amount)* 15/100.0, 2) END),
			(CASE WHEN Commitment_Type IN ('DUE WITHIN 1 WEEK', 'PAST DUE CURRENT', 'BETWEEN 1 WEEK AND A MONTH') THEN '100%' ELSE '15%' END),
			(CASE WHEN Commitment_Type IN ('DUE WITHIN 1 WEEK', 'PAST DUE CURRENT', 'BETWEEN 1 WEEK AND A MONTH') THEN ROUND(SUM(Portion_Amount), 2) ELSE ROUND(SUM(Portion_Amount) * 15/100.0, 2) END)
FROM		MRU_Pipeline_Retail_Detail
GROUP BY	Position_Date, Commitment_Type;

END

GO
/****** Object:  StoredProcedure [dbo].[MRU_Pipeline_Revolving_MakeTable]    Script Date: 2/11/2023 1:33:40 pm ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

 CREATE PROCEDURE [dbo].[MRU_Pipeline_Revolving_MakeTable]	
AS
BEGIN	

    /********************************************************************************************************
	*	PROCEDURE PURPOSE:                                                                                  *
	*	------------------                                                                                  *
	*                                                                                                       *
	* This populates undrawn amount of non-retail revolving facility with following assumption.             *
	*	1.) Retail/Non-Retail customers defined as per MRPU_ProductMapping, classified as non-retail        *
	*	2.) Following accounts excluded as they feed into Liquidity Excel model separately, & are internal  *
	*                                                                                                       *
	*		a.) 1258692: KIWI ASSET FINANCE LIMITED                                                         *
	*                                                                                                       *
	*********************************************************************************************************
	*                                                                                                       *
	* @version	: 1.0                                                                                       *
	* @author	: Stephen Chin                                                                              *
	* @date		: 2020.08.13                                                                                *
	* @details	: Start version control                                                                     * 
	*                                                                                                       *
	*********************************************************************************************************
	*                                                                                                       *
    * @version  : 2.0                                                                                       *
	* @location : S:\dept\Finance\Market Risk and Wholesale Accounting\Model\01_Liquidity\SQL\              *
	* 	          1.19 Undrawn Limit, Pipeline Aggregation, Joint Account\Final SQL - sent to IT (v1.19)    *
	* @author	: Shaun Paul Barnarde'                                                                      *
	* @date	    : 2023.10.16                                                                                *
	*                                                                                                       *
	* 1.) Reference MRU_Product_Mapping, instead of CRE_ExposureRegCalcFull                                 *
	*                                                                                                       *
	* 2.) And is due to CRE_ExposureRegCalcFull assetclass as incorrect base                                *
	*                                                                                                       *
	* 3.) Non retail revolving is defined as, products in productGroup                                      *
	*                                                                                                       *
	*      a.) S30   Business Edge Account, Transactional                                                   *
	*      b.) L30   Business Res Loan Account Variable, Loans, will have undrawn limit if variable and     *
	*                any undrawn component                                                                  *
	*      c.) In both cases above,                                                                         *
	*            i.) if balance = EODLimit, then limit fully drawn                                          *
	*           ii.) if EODLimit id negative and < balance, then not fully drawn,                           *
	*                and undrawn amount = Balance minus EODLimit                                            *
	*                and this results in a positive number                                                  * 
	*                                                                                                       *
	*      d.) S73   Visa Business CC                                                                       *
	*      e.) S78   Business Credit Cards Account                                                          *
	*                                                                                                       *
	*      f.) these could be simplified as 'Produclt Like 'S30%' etc, rather than using ProductGrp         *
	*                                                                                                       *
	* 4.) KAFL is now merged into company structure, since 30-Jun-2023, and therefore excluded.             *
	*                                                                                                       *
	*********************************************************************************************************
	*/

	SET NOCOUNT ON; 

	-- determine the report date
	-- and set the @ReportDate variable

	DECLARE @reportDate DATETIME;
	SET		@reportDate = (Select CONVERT(DATETIME, Variable, 103) From tblVariables Where No = 1);

	-- prepare table
	--

	DELETE	MRU_Pipeline_Revolving_Detail;

	-- -----------------------------------------------------------------------------------------------
	-- STEP 1: business revolving, undrawn limit
	-- -----------------------------------------------------------------------------------------------
	--

	INSERT	MRU_Pipeline_Revolving_Detail (
	
					EODDate, AccessNo, Product, Balance, Limit, Undrawn 
					
					)

			SELECT		EOD.EodDate,
						EOD.AccountNo,
						EOD.Product,
						
						SUM(EOD.EODBalance) AS Balance,
						SUM(EOD.EODLimit)   AS EODLimit,

						CASE WHEN SUM(EOD.EODBalance) < SUM(EOD.EODLimit) THEN 
							0 
						ELSE 
							CASE WHEN SUM(EOD.EODBalance) > 0 THEN 
								-SUM(EOD.EODLimit) 
							ELSE 
								SUM(EOD.EODBalance) - SUM(EOD.EODLimit) 
							END 
						END AS Undrawn
				
				FROM	Ultracs.AccountsEOD EOD,
						MRU_Product_Mapping PM

				WHERE	EODDate = @reportDate
						
					AND PM.ProductGrp = dbo.GetProductGroup (EOD.Product) 

						-- S30   Business Edge Account, Transactional
						-- L30   Business Res Loan Account Variable, Loans
						-- S73   Visa Business CC
						-- S78   Business Credit Cards Account

					AND PM.ProductGrp in ('S30', 'S73', 'S78', 'L30')

						-- KAFL '1258692' is internal, therefore exclude
						--

					AND EOD.AccountNo NOT IN ('1258692')   -- KAFL

				GROUP BY	
						EODDate,
						AccountNo, 
						Product

				HAVING	SUM(EOD.EODLimit)	< 0
					AND	SUM(EOD.EODBalance) < 0
					AND SUM(EOD.EODLimit)	< SUM(EOD.EODBalance)

				ORDER BY	
						AccountNo, Product;

	-- -----------------------------------------------------------------------------------------------
	-- STEP 2: B2K, undrawn limit
	-- -----------------------------------------------------------------------------------------------
	--
			
	INSERT	MRU_Pipeline_Revolving_Detail (
	
					EODDate, AccessNo, Product, Balance, Limit, Undrawn 
					
					)

			SELECT		EODDate,
						AccessNo,
						Product,
						SUM(Balance),
						SUM(Limit),
						SUM(Undrawn)

				FROM
						(
						SELECT	@reportDate AS EODDate,
								[COMP-AI3-CUSTOMER-NUMBER] AS AccessNo,

								CASE WHEN [COMP-PRODUCT(1)] = 'VCC' then 'S73'
										WHEN [COMP-PRODUCT(1)] = 'MCB' then 'S78'
										ELSE [COMP-PRODUCT(1)] END AS Product,

								-[BCM-TOT-NEW-BAL]   AS Balance,
								-[COMP-CREDIT-LIMIT] AS Limit,
					
								([COMP-CREDIT-LIMIT] - [BCM-TOT-NEW-BAL]) AS Undrawn


						FROM   B2K.BCMaster BCM 
								JOIN B2K.BCCompanyMaster BCC ON BCM.[BCM-ACCOUNT-NO] = BCC.[COMP-PROD-BILL-ACCT(1)]

						WHERE  [BCM-PRODUCT-CODE] IN ('MCB', 'VCC')
								AND [BCM-CONSOL-CARD-ACCT-TYPE] = ''
								AND [BCM-TOT-NEW-BAL] > 0

						UNION ALL

						SELECT	@reportDate AS EODDate,
								[COMP-AI3-CUSTOMER-NUMBER] AS AccessNo,

								CASE WHEN [COMP-PRODUCT(2)] = 'VCC' THEN 'S73'
										ELSE [COMP-PRODUCT(1)] END AS Product,

								-[BCM-TOT-NEW-BAL]   AS Balance,
								-[COMP-CREDIT-LIMIT] AS Limit,
					
								([COMP-CREDIT-LIMIT] - [BCM-TOT-NEW-BAL]) AS Undrawn

						FROM   B2K.BCMaster BCM 
								JOIN B2K.BCCompanyMaster BCC ON BCM.[BCM-ACCOUNT-NO] = BCC.[COMP-PROD-BILL-ACCT(2)]

						WHERE  [BCM-PRODUCT-CODE] IN ('MCB', 'VCC')
								AND [BCM-CONSOL-CARD-ACCT-TYPE] = ''
								AND [BCM-TOT-NEW-BAL] > 0

						) tblB2K

				GROUP BY	
						EODDate,
						AccessNo, 
						Product

				HAVING	SUM(Limit)	< 0
					AND	SUM(Balance) < 0
					AND SUM(Limit)	< SUM(Balance)

				ORDER BY 
						AccessNo, Product;


END

GO
/****** Object:  StoredProcedure [dbo].[MRU_RBNZ_NewIssues_Balances_MakeTable]    Script Date: 2/11/2023 1:33:40 pm ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


CREATE PROCEDURE [dbo].[MRU_RBNZ_NewIssues_Balances_MakeTable]
	
AS
BEGIN	
	SET NOCOUNT ON;

/********************************************************************************************************
 * This populates newly issued term funding during the reporting month into MRU_RBNZ_NewIssues_Balances.
 *
 ********************************************************************************************************
 *
 * @version 1.0
 * @author	Stephen Chin
 * @date	2020.08.13
 * 
 * - Start version control
 *
 * @version 1.2: SQL model change 1.11 - Mapping face value to Funding value & adding Carrying Value and CCYCarrying Value
 * @author	My Phan
 * @date	2022.05.24
 * - adding new column
 *		SUM(AD.CarryingValue) AS SumOfCarryingValue
 *
*/

DECLARE	@startDate Date;
DECLARE @endDate Date;
SET		@startDate = (Select ReportStartDate From Variables);
SET		@endDate = (Select ReportEndDate From Variables);

DELETE FROM MRU_RBNZ_NewIssues_Balances;
    
INSERT		MRU_RBNZ_NewIssues_Balances

SELECT		AD.EodDate, 
			AD.FundingType, 
			AD.ClientType, 
			AD.ProductGroup, 
			AD.Residency, 
			CASE WHEN (AD.ClientType = 'FI' OR AD.ClientType LIKE 'INTERNAL%' OR AD.FundingType = 'Market Funding') THEN
				CASE WHEN PM.RBNZProductMapping2 = 'Programme' THEN
					'Programme'
				ELSE
					'FI'
				END
			ELSE
				PM.RBNZProductMapping2
			END AS FundingCategory, 
			CASE WHEN AD.CCY = 'NZD' THEN 'NZD' ELSE 'Non-NZD' END AS [CCY Category], 
			PM.RBNZProductMapping2, 
			dbo.GetBucketProfileFundingRBNZ(dbo.GetTimeProfile(ISNULL(AD.LodegementDate, @startDate), ISNULL(AD.OriginalTermDays, 0))) AS RBNZInitialMaturityBucket, 
			SUM(AD.Balance) AS SumOfBalance, 
			SUM(AD.FaceValue) AS SumOfFaceValue, 
			SUM(AD.BookValue) AS SumOfBookValue, 
			SUM(AD.MarketValue) AS SumOfMarketValue,
			-- v1.2 add new column
			SUM(AD.CarryingValue) AS SumOfCarryingValue
	
FROM		AllData AD 
			LEFT JOIN MRU_Product_Mapping PM ON AD.ProductGroup = PM.ProductGrp
	
WHERE		PM.ProductType = 'Term' 
			AND AD.LodegementDate BETWEEN @startDate AND @endDate
	
GROUP BY	AD.EodDate, 
			AD.FundingType, 
			AD.ClientType, 
			AD.ProductGroup, 
			AD.Residency, 
			CASE WHEN (AD.ClientType = 'FI' OR AD.ClientType LIKE 'INTERNAL%' OR AD.FundingType = 'Market Funding') THEN
				CASE WHEN PM.RBNZProductMapping2 = 'Programme' THEN
					'Programme'
				ELSE
					'FI'
				END
			ELSE
				PM.RBNZProductMapping2
			END, 
			CASE WHEN AD.CCY = 'NZD' THEN 'NZD' ELSE 'Non-NZD' END, 
			PM.RBNZProductMapping2, 
			dbo.GetBucketProfileFundingRBNZ(dbo.GetTimeProfile(ISNULL(AD.LodegementDate, @startDate), ISNULL(AD.OriginalTermDays, 0))),
			PM.ProductType, 
			AD.LodegementDate
		
ORDER BY	AD.ClientType, AD.ProductGroup;

END

GO
/****** Object:  StoredProcedure [dbo].[MRU_RBNZ_NewIssues_WAC_MakeTable]    Script Date: 2/11/2023 1:33:40 pm ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


CREATE PROCEDURE [dbo].[MRU_RBNZ_NewIssues_WAC_MakeTable]
	
AS
BEGIN
	
	SET NOCOUNT ON;

/********************************************************************************************************
 * This populates newly issued term funding during the reporting month associated with margin into MRU_RBNZ_NewIssues_Balances.
 *
 ********************************************************************************************************
 *
 * @version 1.0
 * @author	Stephen Chin
 * @date	2020.08.13
 * 
 * - Start version control
 *
 * @version 1.2: SQL model change 1.11
 * @author	My Phan
 * @date	2022.10.18
 * adding: CarryingValue
 * 
 *
*/

DECLARE	@startDate Date;
DECLARE @endDate Date;
SET		@startDate = (Select ReportStartDate From Variables);
SET		@endDate = (Select ReportEndDate From Variables);

DELETE FROM MRU_RBNZ_NewIssues_WAC;

INSERT		MRU_RBNZ_NewIssues_WAC

SELECT		EodDate, 
			GroupAccountNo, 
			AccountNo, 
			AccountName, 
			Product, 
			ProductGroup, 
			TradeableFlag, 
			Banding, 
			ClientType, 
			RBNZGrouping, 
			FundingType, 
			FundingCategory, 
			Residency, 
			[CCY Category], 
			CCY, 
			RBNZProductMapping2, 
			LodegementDate, 
			MaturityDate, 
			OriginalTermDays, 
			RemaingTermDays, 
			RBNZInitialMaturityBucket, 
			RatesTerm, 
			WholesaleRate, 
			ClientInterestFrequency, 
			Rate AS ClientRate, 
			Margin, 
			Balance, 
			FaceValue, 
			BookValue, 
			MarketValue, 
			Margin * FaceValue AS MarginWeight,
			-- v1.2 add new column
			CarryingValue
FROM		(
			SELECT	*,
					CASE WHEN Margin_BKBM IS NOT NULL THEN 
						Margin_BKBM
					ELSE
						CASE WHEN CCY <> 'NZD' THEN 0 ELSE Rate - WholesaleRate END END AS Margin

			FROM	(	
					SELECT	*,
							FundingType + FundingCategory + Residency + [CCY Category] AS RBNZGrouping,
							CASE WHEN CCY <> 'NZD' THEN
								0
							ELSE
								CASE WHEN OriginalTermDays < 457 THEN
									CASE WHEN RatesTerm = 'Overnight' THEN
										Overnight
									ELSE
										CASE RatesTerm
											WHEN '30' THEN [30]
											WHEN '60' THEN [60] 
											WHEN '90' THEN [90]
											WHEN '100' THEN [100]
											WHEN '120' THEN [120]
											WHEN '150' THEN [150]
											WHEN '180' THEN	[180]
											WHEN '270' THEN	[270]
											WHEN '365' THEN	[365]
										ELSE
											Overnight
										END
									END							
								ELSE
									CASE RatesTerm
										WHEN '548' THEN [548]
										WHEN '730' THEN [730]
										WHEN '913' THEN [913]
										WHEN '1095' THEN [1095]
										WHEN '1460' THEN [1460] 
										WHEN '1825' THEN [1825]
										WHEN '2555' THEN [2555]
										WHEN '3650' THEN [3650]
									ELSE
										Overnight
									END						
								END
							END AS WholesaleRate
					FROM	(
							SELECT	*,
									dbo.GetBucketProfileFundingRBNZ(dbo.GetTimeProfile(ISNULL(LodegementDate, @startDate), ISNULL(OriginalTermDays, 0))) AS RBNZInitialMaturityBucket, 
									dbo.GetTimeProfileHistRates(OriginalTermDays) AS RatesTerm, 
									CASE WHEN ClientType = 'FI' OR ClientType LIKE 'INTERNAL%' OR FundingType = 'Market Funding' THEN
										CASE WHEN RBNZProductMapping2 = 'Programme' THEN 'Programme' ELSE 'FI' END
									ELSE
										RBNZProductMapping2
									END AS FundingCategory,
									CASE WHEN CCY = 'NZD' THEN 'NZD' ELSE 'Non-NZD' END AS [CCY Category]
							FROM	(
									SELECT	AD.EodDate, 
											AD.GroupAccountNo, 
											AD.AccountNo, 
											AD.AccountName, 
											AD.Product, 
											AD.ProductGroup, 
											AD.TradeableFlag, 
											AD.Banding, 
											AD.ClientType, 
											AD.FundingType, 
											AD.Residency, 
											AD.CCY, 
											PM.RBNZProductMapping2, 
											AD.LodegementDate, 
											AD.MaturityDate, 
											AD.OriginalTermDays, 
											AD.RemaingTermDays, 
											AD.IntFreq AS ClientInterestFrequency, 
											AD.Rate, 
											AD.Balance, 
											AD.FaceValue, 
											AD.BookValue, 
											AD.MarketValue,
											HR.*,
											NPI.Margin_BKBM,
											AD.CarryingValue
									FROM	AllData AD
											LEFT JOIN MRU_Product_Mapping PM ON AD.ProductGroup = PM.ProductGrp 
											LEFT JOIN HistRates HR ON AD.LodegementDate = HR.FundingDate
											LEFT JOIN MRU_RBNZ_New_Programme_Issues NPI ON AD.Source = NPI.Source AND AD.Identification = NPI.Identification AND AD.Product = NPI.Product AND AD.AccountNo = NPI.[Account No] AND AD.LodegementDate = NPI.[Lodgement Date] AND AD.MaturityDate = NPI.[Maturity Date]
									WHERE	AD.LodegementDate BETWEEN @startDate AND @endDate 
											AND PM.ProductType = 'Term'
									) Detail0
							) Detail1
						) Detail2
					) Detail3
ORDER BY ClientType, ProductGroup;

END

GO
/****** Object:  StoredProcedure [dbo].[MRU_Retail_Card_MakeTable]    Script Date: 2/11/2023 1:33:40 pm ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


CREATE PROCEDURE [dbo].[MRU_Retail_Card_MakeTable]	
AS
BEGIN
	
SET NOCOUNT ON;

/********************************************************************************************************
 * This populates an interim table MRU_Retail_Card_InFunds which stores credit card in-funds balance 
 * sourced from input tables B2K.BCMaster and B2K.BCCompanyMaster.
 *
 * S71: Visa Retail
 * S72: Visa Airpoints
 * S73: Business Visa
 * S83: Mastercard Airpoints
 * S87: Mastercard Retail
 * S78: Business Card
 * S77: Personal Loan
 *
 ********************************************************************************************************
 *
 * @version 1.0
 * @author	Stephen Chin
 * @date	2020.08.13
 * 
 * - Start version control
 *
 * -----------------------------------------
 *
 * @version 1.1
 * @author	Stephen Chin
 * @date	2021.09.03
 * 
 * - Add new business visa product and map to S73
 *
 *
*/

TRUNCATE TABLE MRU_Retail_Card_InFunds;

DECLARE @reportDate DATETIME;
SET		@reportDate = (Select CONVERT(DATETIME, Variable, 103) From tblVariables Where No = 1);

INSERT	MRU_Retail_Card_InFunds

SELECT	@reportDate AS EODDate,
        SUBSTRING([BCM-CARDHOLDER-NBR], PATINDEX('%[^0]%', [BCM-CARDHOLDER-NBR]+'.'), LEN([BCM-CARDHOLDER-NBR])) AS AccessNo,
        CASE WHEN [BCM-PRODUCT-CODE] + [BCM-SUB-PRODUCT-CODE] IN ('MCSSTD', 'MCSNFC', 'MCGGLD') THEN 'S87'
			 WHEN [BCM-PRODUCT-CODE] + [BCM-SUB-PRODUCT-CODE] IN ('VCSVLR', 'VCSVNF', 'VCPVPL') THEN 'S71'
			 WHEN [BCM-PRODUCT-CODE] + [BCM-SUB-PRODUCT-CODE] IN ('MCSAPS', 'MCSAPL', 'MPLAPP') THEN 'S83'
			 WHEN [BCM-PRODUCT-CODE] + [BCM-SUB-PRODUCT-CODE] IN ('VCSVAS', 'VCSVAL', 'VCPVAP') THEN 'S72'
             WHEN [BCM-PRODUCT-CODE] IN ('KPL') THEN 'S77'
			 ELSE [BCM-PRODUCT-CODE] + [BCM-SUB-PRODUCT-CODE] END AS Product,
        [BCM-TOT-NEW-BAL] AS EODBalance,
        CASE WHEN [BCM-TOT-NEW-BAL] < 0 THEN -[BCM-TOT-NEW-BAL] ELSE 0 END AS InFundsAmount

FROM   B2K.BCMaster

WHERE  [BCM-CONSOL-CARD-ACCT-TYPE] = 'A'

UNION ALL

-- Business
SELECT	@reportDate AS EODDate,
        [COMP-AI3-CUSTOMER-NUMBER] AS AccessNo,
        CASE WHEN [COMP-PRODUCT(1)] = 'VCC' then 'S73'
             WHEN [COMP-PRODUCT(1)] = 'MCB' then 'S78'
             ELSE [COMP-PRODUCT(1)] END AS Product,

        [BCM-TOT-NEW-BAL] AS EODBalance,

        CASE WHEN [BCM-TOT-NEW-BAL] < 0 THEN -[BCM-TOT-NEW-BAL]
             ELSE 0 END AS InFundsAmount

FROM   B2K.BCMaster BCM 
       JOIN B2K.BCCompanyMaster BCC ON BCM.[BCM-ACCOUNT-NO] = BCC.[COMP-PROD-BILL-ACCT(1)]

WHERE  [BCM-PRODUCT-CODE] IN ('MCB', 'VCC')
       AND [BCM-CONSOL-CARD-ACCT-TYPE] = ''

UNION ALL

SELECT	@reportDate AS EODDate,
        [COMP-AI3-CUSTOMER-NUMBER] AS AccessNo,
        CASE WHEN [COMP-PRODUCT(2)] = 'VCC' THEN 'S73'
             ELSE [COMP-PRODUCT(1)] END AS Product,

        [BCM-TOT-NEW-BAL] AS EODBalance,

        CASE WHEN [BCM-TOT-NEW-BAL] < 0 THEN -[BCM-TOT-NEW-BAL]
             ELSE 0 END AS InFundsAmount

FROM   B2K.BCMaster BCM 
       JOIN B2K.BCCompanyMaster BCC ON BCM.[BCM-ACCOUNT-NO] = BCC.[COMP-PROD-BILL-ACCT(2)]

WHERE  [BCM-PRODUCT-CODE] IN ('MCB', 'VCC')
       AND [BCM-CONSOL-CARD-ACCT-TYPE] = '';

END

GO
/****** Object:  StoredProcedure [dbo].[MRU_Retail_ClientGroup_Mapping_MakeTable]    Script Date: 2/11/2023 1:33:40 pm ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[MRU_Retail_ClientGroup_Mapping_MakeTable]
AS
BEGIN
SET NOCOUNT ON;
    
-- Update TMS Process Lock Date to Next Business Day for operational control
UPDATE tblVariables
SET Variable = CONVERT(VARCHAR(10), Calendar.dbo.AdvanceBusinessDays(GETDATE(),1,'NZ'), 103) WHERE No = 3

/********************************************************************************************************
 * This populates an interim table MRU_Retail_ClientGroup_Mapping based on customers and their business link input tables.
 * 
 *
 ********************************************************************************************************
 *
 * @version 1.0
 * @author	Stephen Chin
 * @date	2020.08.13
 * 
 * - Start version control
 *
*/
      
TRUNCATE TABLE MRU_Retail_ClientGroup_Mapping;

INSERT INTO MRU_Retail_ClientGroup_Mapping 
			(GroupAccountNo, GroupName, AccountNo, UltracsCounterparty, ClientType, RBNZResidencyMapping1)

SELECT	CBL.AccessNo AS GroupAccountNo, 
		CS.Surname AS GroupName, 
		CBL.LinkToAccessNo AS AccountNo, 
		ISNULL(CS_LinkTo.Surname,'') + ' ' + ISNULL(CS_LinkTo.Forenames,'') AS UltracsCounterparty, 
		NULL AS ClientType, 
		CASE WHEN CS_LinkTo.WHtaxExempt = '2' THEN 'Offshore' ELSE 'Domestic' END AS RBNZResidencyMapping1
FROM	CustomersBusinessLinks CBL
        LEFT JOIN CustomerStatic CS ON CBL.AccessNo = CS.AccessNo
        LEFT JOIN CustomerStatic CS_LinkTo ON CBL.LinkToAccessNo = CS_LinkTo.AccessNo
WHERE	CBL.LinkType = 'M';

END

GO
/****** Object:  StoredProcedure [dbo].[MRU_Retail_Loan_Cashflows_MakeTable]    Script Date: 2/11/2023 1:33:40 pm ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO




CREATE PROCEDURE [dbo].[MRU_Retail_Loan_Cashflows_MakeTable]  
AS
BEGIN  

SET NOCOUNT ON;

/********************************************************************************************************
 * This slices consolidated principal loan cashflow of input table, MRU_Retail_Loan_Cashflows into detailed time buckets.
 * Following are excluded from the cashflow:
 * - Revolving product (L6 and L8)
 * - In-arrears; Account in-arrears is defined as arrears days is greater than or equal to 90 days. This applies to account level, not indvidual products.
 * - Repayment holiday; Covid19 related mortgage deferral scheme is implemented in MRU_Retail_Loan_RepayHoliday table.
 *						RepayHolidayIndType 'C' and 'O' represents 'Repayment holiday due to Covid19' and 'Interest only due to Covid19'.
 *						Note that cashflow from repayment holiday due to standard hardship had been already excluded in preprocessed staging view.
 *
 ********************************************************************************************************
 *
 * @version 1.0
 * @author	Stephen Chin
 * @date	2020.08.13
 * 
 * - Start version control
 *
 *
*/
TRUNCATE TABLE MRU_Retail_Loan_RepayHoliday;

DECLARE @reportDate DATETIME;
SET	@reportDate = (Select CONVERT(DATETIME, Variable, 103) From tblVariables Where No = 1);

WITH Temp AS
(
SELECT ROW_NUMBER() OVER (PARTITION BY Account, Product ORDER BY Seq) AS 'nRow', * FROM
(
Select	AL.*, 
		ALR.Condition, ALR.EffectiveDate, ALR.ExpiryDate, ALR.Rate, ALR.RepayMethod, ALR.Seq
From	Ultracs.UltracsAccountLoans AL
		LEFT JOIN Ultracs.UltracsAccountLoanRates ALR ON AL.Account = ALR.Account AND AL.Product = ALR.Product
Where	(AL.RepayHolidayIndType IN ('C') AND CONVERT(date, @reportDate) BETWEEN AL.RepayHolidayAppliesFrom AND AL.RepayHolidayAppliesTo AND CONVERT(date, @reportDate) <= AL.NextRepayDate)
		OR
		(AL.RepayHolidayIndType IN ('O') AND CONVERT(date, @reportDate) BETWEEN ALR.EffectiveDate AND ALR.Expirydate)
) a
)

INSERT	MRU_Retail_Loan_RepayHoliday

SELECT	Account, Product, NextRepayAmt, NextRepayFreq, NextRepayDate, RepayHolidayIndType, RepayHolidayAppliesFrom, RepayHolidayAppliesTo, 
		RepayMethod, EffectiveDate, Expirydate
FROM	Temp 
WHERE	nRow = 1;


TRUNCATE TABLE MRU_Retail_Loan_Cashflows_Result

INSERT      MRU_Retail_Loan_Cashflows_Result

SELECT		EODDate,
			ProductGroup,
			SUM(Balance) AS Balance,
			SUM([1Day]) AS [1Day],
			SUM([2Day]) AS [2Day],
			SUM([3Day]) AS [3Day],
			SUM([4Day]) AS [4Day],
			SUM([5Day]) AS [5Day],
			SUM([6Day]) AS [6Day],
			SUM([7Day]) AS [7Day],
			SUM([14Day]) AS [14Day],
			SUM([1Month]) AS [1Month],
			SUM([2Month]) AS [2Month],
			SUM([3Month]) AS [3Month],
			SUM([6Month]) AS [5Month],
			SUM([1Year]) AS [1Year]
FROM		(
			SELECT		@reportDate AS EODDate, 
						dbo.GetProductGroup(LN.Product) AS ProductGroup, 
						SUM(LN.StartBalance) AS Balance, 
						SUM(LN.[1dayPrincipal]) AS [1Day], 
						SUM(LN.[2dayPrincipal]) AS [2Day], 
						SUM(LN.[3dayPrincipal]) AS [3Day], 
						SUM(LN.[4dayPrincipal]) AS [4Day], 
						SUM(LN.[5dayPrincipal]) AS [5Day], 
						SUM(LN.[6dayPrincipal]) AS [6Day], 
						SUM(LN.[7dayPrincipal]) AS [7Day], 
						SUM(LN.[8-14dayPrincipal]) AS [14Day], 
						SUM(LN.[15day-1monthPrincipal]) AS [1Month], 
						SUM(LN.[1-2monthPrincipal]) AS [2Month], 
						SUM(LN.[2-3monthPrincipal]) AS [3Month], 
						SUM(LN.[3-6monthPrincipal]) AS [6Month], 
						SUM(LN.[6month-1YearPrincipal]) AS [1Year] 
			FROM		MRU_Retail_Loan_Cashflows LN
			WHERE       LN.Account NOT IN (Select Distinct AccessNo From Ultracs.Arrears Where Product LIKE 'L%' AND ArrearsAmt > 0 AND ArrearsDays >= 90)
						AND (LN.Product <> 'L6' AND LN.Product NOT LIKE 'L6.%')
						AND (LN.Product <> 'L8' AND LN.Product NOT LIKE 'L8.%')
			GROUP BY    dbo.GetProductGroup(LN.Product)

			UNION ALL

			SELECT		@reportDate AS EODDate, 
						dbo.GetProductGroup(LN.Product) AS ProductGroup, 
						-SUM(LN.StartBalance) AS Balance, 
						-SUM(CASE WHEN LN.[1dayPrincipal] < 0 THEN 0 ELSE LN.[1dayPrincipal] END) AS [1Day],
						-SUM(CASE WHEN LN.[2dayPrincipal] < 0 THEN 0 ELSE LN.[2dayPrincipal] END) AS [2Day],
						-SUM(CASE WHEN LN.[3dayPrincipal] < 0 THEN 0 ELSE LN.[3dayPrincipal] END) AS [3Day], 
						-SUM(CASE WHEN LN.[4dayPrincipal] < 0 THEN 0 ELSE LN.[4dayPrincipal] END) AS [4Day], 
						-SUM(CASE WHEN LN.[5dayPrincipal] < 0 THEN 0 ELSE LN.[5dayPrincipal] END) AS [5Day], 
						-SUM(CASE WHEN LN.[6dayPrincipal] < 0 THEN 0 ELSE LN.[6dayPrincipal] END) AS [6Day], 
						-SUM(CASE WHEN LN.[7dayPrincipal] < 0 THEN 0 ELSE LN.[7dayPrincipal] END) AS [7Day],  
						-SUM(CASE WHEN LN.[8-14dayPrincipal] < 0 THEN 0 ELSE LN.[8-14dayPrincipal] END) AS [14Day], 
						-SUM(CASE WHEN LN.[15day-1monthPrincipal] < 0 THEN 0 ELSE LN.[15day-1monthPrincipal] END) AS [1Month], 
						-SUM(CASE WHEN LN.[1-2monthPrincipal] < 0 THEN 0 ELSE LN.[1-2monthPrincipal] END) AS [2Month], 
						-SUM(CASE WHEN LN.[2-3monthPrincipal] < 0 THEN 0 ELSE LN.[2-3monthPrincipal] END) AS [3Month], 
						-SUM(CASE WHEN LN.[3-6monthPrincipal] < 0 THEN 0 ELSE LN.[3-6monthPrincipal] END) AS [6Month], 
						-SUM(CASE WHEN LN.[6month-1YearPrincipal] < 0 THEN 0 ELSE LN.[6month-1YearPrincipal] END) AS [1Year] 
			FROM		(
						Select	CF.*
						From	MRU_Retail_Loan_Cashflows CF 
								LEFT JOIN MRU_Retail_Loan_RepayHoliday RH ON CF.Account = RH.Account AND CF.Product = RH.Product
						Where	CF.CurrentInterestRepaymethod = 'PI'
								AND RH.Account IS NOT NULL
						) LN
			WHERE       LN.Account NOT IN (Select Distinct AccessNo From Ultracs.Arrears Where Product LIKE 'L%' AND ArrearsAmt > 0 AND ArrearsDays >= 90)
						AND (LN.Product <> 'L6' AND LN.Product NOT LIKE 'L6.%')
						AND (LN.Product <> 'L8' AND LN.Product NOT LIKE 'L8.%')
			GROUP BY    dbo.GetProductGroup(LN.Product)
			) unionall
GROUP BY	EODDate, ProductGroup
ORDER BY	EODDate, ProductGroup;

END

GO
/****** Object:  StoredProcedure [dbo].[MRU_Retail_Saving_Cashflows_MakeTable]    Script Date: 2/11/2023 1:33:40 pm ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


CREATE PROCEDURE [dbo].[MRU_Retail_Saving_Cashflows_MakeTable]
AS
BEGIN	

/********************************************************************************************************
* This populates interest payment from Notice Saver into MRU_Retail_Saving_Cashflows_Detail.
* Then MRU_Retail_Saving_Cashflows_Summary has aggregated total interest cashflow.
* Other saving products do not allow interest being paid to external bank account directly according to current business rule.
* Hence, Notice Saver is unique product that generates cashflow (out) among non-TD savings.
*
* Note:
* 1. AccInt/10 is correct decemal place in dollar.
*
* 2. Current interest rate of Notice Saver sourced from Ultracs.NoticeSaverRate depending on NPAcode (32D or 90D).
*      If the NPAcode is missing then 90D interest rate applies.
*
* 3. The minimum balance to earn interest in Notice Saver is assigned to variable @s8_threshold.
*      At the time of writing, it sets to $0. Update the variable if required.
*
* 4. Assumed that interest payment at the end of month equals to sum of accrual interest and estimated interest based on current EOD balance.
*      AccInt: Accruded interest
*      EstInt: Estimated interest
*       TotalInt = AccInt + EstInt
*
* 5. Internal/External logic
*      If NetIntPostFlg = 'Y' and non Kiwibank account appears on NominatedAccount table then 'External'.
*      Otherwise, 'Internal'.
*
* 6. Payment of interest bearing on offset account is excluded.
*
********************************************************************************************************
*
* @version 1.0
* @author    Stephen Chin
* @date    2020.08.13
*
* - Start version control
* - Add new time bucket profile '50Y' to [1YOver]
*
*********************************************************************************************************
* @version1.12
* @author Hien Le
* @date 2022.08.30
* -    Update @s8_threshold change from $2000 to $0, effective 30 Aug 2022 (product change effective 24 August 2022 – non-material impact ).
*/

SET NOCOUNT ON;

Declare @rptdate date, @sdate date, @edate date, @nexteom date
Declare @days_acc int, @days_est int, @days_eom int
Declare @dcf_acc float, @dcf_est float, @s8_threshold float, @dcf_eom float

Set @rptdate = (Select CONVERT(DATE, Variable, 103) From tblVariables Where No = 1)
Set @sdate = convert(date, DATEADD(month, DATEDIFF(month, 0, @rptdate), 0))
Set @edate = convert(date, DATEADD(month, DATEDIFF(month, 0, @rptdate)+1, -1))
Set @nexteom = convert(date, DATEADD(month, DATEDIFF(month, 0, @rptdate)+2, -1))
Set @days_acc = DATEDIFF(DD, @sdate, @rptdate) + 1
Set @days_est = DATEDIFF(DD, @rptdate, @edate)
Set @days_eom = DATEDIFF(DD, @rptdate, @nexteom) - 1
Set @dcf_acc = @days_acc / 365.0 
Set @dcf_est = @days_est / 365.0
Set @dcf_eom = @days_eom / 365.0
Set @s8_threshold = 0


TRUNCATE TABLE MRU_Retail_Saving_Cashflows_Detail;

INSERT	MRU_Retail_Saving_Cashflows_Detail

SELECT	EOD.EodDate, EOD.AccountNo, EOD.Product, EOD.ProductGroup, EOD.EodBalance,
		ISNULL(NS.Rate, (Select Rate From Ultracs.NoticeSaverRate Where NoticeCode = '90D')) AS BaseRate,
		ISNULL(ACC.AccInt, 0.0) / 10.0 AS AccInt,
		CASE WHEN (EOD.EodBalance < @s8_threshold) OR (OS.Offset1_AccessNo IS NOT NULL) THEN 
			0 
		ELSE 
			CASE WHEN @days_est = 0 THEN 
				EOD.EODBalance * ISNULL(NS.Rate, (Select Rate From Ultracs.NoticeSaverRate Where NoticeCode = '90D')) / 100.0 * @dcf_eom
			ELSE
				EOD.EODBalance * ISNULL(NS.Rate, (Select Rate From Ultracs.NoticeSaverRate Where NoticeCode = '90D')) / 100.0 * @dcf_est
			END
		END AS EstInt,
		
		CASE WHEN (EOD.EodBalance < @s8_threshold) OR (OS.Offset1_AccessNo IS NOT NULL) THEN 
			ISNULL(ACC.AccInt, 0) / 10.0
		ELSE 
			CASE WHEN @days_est = 0 THEN
				ISNULL(ACC.AccInt, 0) / 10.0 + EOD.EODBalance * ISNULL(NS.Rate, (Select Rate From Ultracs.NoticeSaverRate Where NoticeCode = '90D')) / 100.0 * @dcf_eom
			ELSE
				ISNULL(ACC.AccInt, 0) / 10.0 + EOD.EODBalance * ISNULL(NS.Rate, (Select Rate From Ultracs.NoticeSaverRate Where NoticeCode = '90D')) / 100.0 * @dcf_est
			END
		END AS TotalInt,
		
		CASE WHEN (ACC.NetIntPostFlg = 'Y' AND NOM.BankBranch NOT LIKE '38%') THEN 'External' ELSE 'Internal' END AS Destination

FROM	(Select EodDate, AccountNo, Product, EodBalance, dbo.GetProductGroup(Product) As ProductGroup From Ultracs.AccountsEOD Where EODBalance > 0) EOD
		LEFT JOIN Ultracs.Account ACC ON EOD.AccountNo = ACC.AccessNo AND EOD.Product = ACC.Product
		LEFT JOIN Ultracs.NoticeSaverRate NS ON ACC.NPAcode = NS.NoticeCode
		LEFT JOIN Ultracs.OnlineCallNominatedAccount NOM ON EOD.AccountNo = NOM.AccessNo AND EOD.Product = NOM.Product
		LEFT JOIN Ultracs.AccountOffset OS ON EOD.AccountNo = OS.AccessNo AND EOD.Product = OS.Product			
WHERE	EOD.ProductGroup IN ('S8');


Set @edate = CASE WHEN @days_est = 0 THEN @nexteom Else convert(date, DATEADD(MM, DATEDIFF(MM, 0, @rptdate)+1, -1)) END

TRUNCATE TABLE MRU_Retail_Saving_Cashflows_Summary;

INSERT		MRU_Retail_Saving_Cashflows_Summary

SELECT		EodDate, 
			ProductGroup,
			SUM(EodBalance) AS Balance, 
			CASE WHEN dbo.GetTimeProfile(@rptdate, DATEDIFF(DD, @rptdate, @edate)) IN ('0D', '1D') THEN SUM(TotalInt) ELSE 0 END AS [1D],
			CASE WHEN dbo.GetTimeProfile(@rptdate, DATEDIFF(DD, @rptdate, @edate)) = '2D' THEN SUM(TotalInt) ELSE 0 END AS [2D],
			CASE WHEN dbo.GetTimeProfile(@rptdate, DATEDIFF(DD, @rptdate, @edate)) = '3D' THEN SUM(TotalInt) ELSE 0 END AS [3D],
			CASE WHEN dbo.GetTimeProfile(@rptdate, DATEDIFF(DD, @rptdate, @edate)) = '4D' THEN SUM(TotalInt) ELSE 0 END AS [4D],
			CASE WHEN dbo.GetTimeProfile(@rptdate, DATEDIFF(DD, @rptdate, @edate)) = '5D' THEN SUM(TotalInt) ELSE 0 END AS [5D],
			CASE WHEN dbo.GetTimeProfile(@rptdate, DATEDIFF(DD, @rptdate, @edate)) = '6D' THEN SUM(TotalInt) ELSE 0 END AS [6D],
			CASE WHEN dbo.GetTimeProfile(@rptdate, DATEDIFF(DD, @rptdate, @edate)) = '1W' THEN SUM(TotalInt) ELSE 0 END AS [1W],
			CASE WHEN dbo.GetTimeProfile(@rptdate, DATEDIFF(DD, @rptdate, @edate)) = '2W' THEN SUM(TotalInt) ELSE 0 END AS [2W],
			CASE WHEN dbo.GetTimeProfile(@rptdate, DATEDIFF(DD, @rptdate, @edate)) IN ('3W', '1M') THEN SUM(TotalInt) ELSE 0 END AS [1M],
			CASE WHEN dbo.GetTimeProfile(@rptdate, DATEDIFF(DD, @rptdate, @edate)) = '2M' THEN SUM(TotalInt) ELSE 0 END AS [2M],
			CASE WHEN dbo.GetTimeProfile(@rptdate, DATEDIFF(DD, @rptdate, @edate)) = '3M' THEN SUM(TotalInt) ELSE 0 END AS [3M],
			CASE WHEN dbo.GetTimeProfile(@rptdate, DATEDIFF(DD, @rptdate, @edate)) IN ('4M', '5M', '6M') THEN SUM(TotalInt) ELSE 0 END AS [6M],
			CASE WHEN dbo.GetTimeProfile(@rptdate, DATEDIFF(DD, @rptdate, @edate)) = '1Y' THEN SUM(TotalInt) ELSE 0 END AS [1Y],
			CASE WHEN dbo.GetTimeProfile(@rptdate, DATEDIFF(DD, @rptdate, @edate)) IN ('2Y', '3Y', '4Y', '5Y', '7Y', '10Y', '15Y', '20Y', '50Y') THEN SUM(TotalInt) ELSE 0 END AS [1YOver]

FROM		MRU_Retail_Saving_Cashflows_Detail
WHERE		Destination = 'External'
GROUP BY	EodDate, ProductGroup
ORDER BY	ProductGroup;

END

GO
/****** Object:  StoredProcedure [dbo].[MRU_Retail_TD_Cashflows_MakeTable]    Script Date: 2/11/2023 1:33:40 pm ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[MRU_Retail_TD_Cashflows_MakeTable]
AS
BEGIN	
SET NOCOUNT ON;
/********************************************************************************************************
 * This slices consolidated term deposit interest payment of an input table MRU_Retail_TD_Cashflows into detailed time buckets.
 * Note that only payment to external bank account is taken into account. 
 *
 * Maturity Instruction on PaymentIns:
 * 0 = Payout to KB account
 * 2 = Reinvest TD
 * 4 = Payout to external account
 *
 ********************************************************************************************************
 *
 * @version 1.0
 * @author	Stephen Chin
 * @date	2020.08.13
 * 
 * - Start version control
 *
 *
*/

TRUNCATE TABLE MRU_Retail_TD_Cashflows_Result;

INSERT		MRU_Retail_TD_Cashflows_Result

SELECT		DT.ReportEndDate AS EODDate,
			dbo.GetProductGroup(CF.product) AS ProductGroup,
			SUM(CF.amount) AS Balance,
			SUM(CF.[1dayInterest]) AS [1Day],
			SUM(CF.[2dayInterest]) AS [2Day], 
			SUM(CF.[3dayInterest]) AS [3Day], 
			SUM(CF.[4dayInterest]) AS [4Day], 
			SUM(CF.[5dayInterest]) AS [5Day], 
			SUM(CF.[6dayInterest]) AS [6Day], 
			SUM(CF.[7dayInterest]) AS [7Day], 
			SUM(CF.[8-14dayInterest]) AS [14Day], 
			SUM(CF.[15day-1monthInterest]) AS [1Month], 
			SUM(CF.[1-2monthInterest]) AS [2Month], 
			SUM(CF.[2-3monthInterest]) AS [3Month], 
			SUM(CF.[3-6monthInterest]) AS [6Month], 
			SUM(CF.[6month-1YearInterest]) AS [1Year],
			SUM(CF.[1-2YearInterest] + CF.[2-3YearInterest] + CF.[3-4YearInterest] + CF.[4-5YearInterest] + CF.[5-7YearInterest] + CF.[7-10YearInterest] + CF.Over10YearsInterest) AS [Over1Year]
FROM		MRU_Retail_TD_Cashflows CF
			LEFT JOIN  Ultracs.TermDeposit TD ON CF.accounts = TD.AccessNo AND CF.product = TD.Product,
			Variables DT
WHERE		TD.PaymentInsI = 4
GROUP BY	DT.ReportEndDate,
			dbo.GetProductGroup(CF.product);

END

GO
/****** Object:  StoredProcedure [dbo].[MRU_TMS_Liquidity_Cashflows_Update]    Script Date: 2/11/2023 1:33:40 pm ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[MRU_TMS_Liquidity_Cashflows_Update]	
AS
BEGIN
	
SET NOCOUNT ON;
/********************************************************************************************************
 * The procedure updates MRU_TMS_Liquidity_Cashflows table.
 * i.e., Override end date and its related fields with effective maturity date of Funding for Lending Program (FLP) deals
 * Note this is only applicable to MM side of the deal.
 *
 ********************************************************************************************************
 *
 * @version 1.0
 * @author	Stephen Chin
 * @date	2021.01.13
 * 
 * - Initial version
 *
 *
*/

DECLARE @sDate DATETIME;
SET	@sDate = (Select CONVERT(DATETIME, Variable, 103) From tblVariables Where No = 1);

UPDATE	MRU_TMS_Liquidity_Cashflows

SET		[Dealt Date] = FLP.DealtDate,
		[Begin Date] = FLP.BeginDate,
		[End Date] = FLP.EndDate,
		[Cash Flow Date] = FLP.EndDate,
		[Days To Maturity] = DATEDIFF(D, @sDate, FLP.EndDate),
		[Adj Time Profile] = dbo.GetTimeProfileString(@sDate, FLP.EndDate)
FROM	MRU_TMS_Liquidity_Cashflows RPT
		INNER JOIN MRU_TMS_Deal_Override_FLP FLP ON RPT.[Deal Id] = FLP.DealId AND RPT.[D1Deal No] = FLP.DealNo
WHERE	RPT.[Transaction Type] = 'MM'
		AND RPT.Instrument LIKE 'Repo FLP%';

END

GO
/****** Object:  StoredProcedure [dbo].[MRU_TMS_Liquidity_Reporting_Update]    Script Date: 2/11/2023 1:33:40 pm ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[MRU_TMS_Liquidity_Reporting_Update]	
AS
BEGIN
	
SET NOCOUNT ON;
/********************************************************************************************************
 * The procedure updates MRU_TMS_Liquidity_Reporting table.
 * i.e., Override end date and its related fields with effective maturity date of Funding for Lending Program (FLP) deals
 *
 ********************************************************************************************************
 *
 * @version 1.0
 * @author	Stephen Chin
 * @date	2021.01.13
 * 
 * - Initial version
 *
 *
*/

DECLARE @sDate DATETIME;
SET	@sDate = (Select CONVERT(DATETIME, Variable, 103) From tblVariables Where No = 1);
		
UPDATE	MRU_TMS_Liquidity_Reporting

SET		[Dealt Date] = FLP.DealtDate,
		[Begin Date] = FLP.BeginDate,
		[End Date] = FLP.EndDate,
		[Days To Maturity] = DATEDIFF(D, @sDate, FLP.EndDate),
		[Adj Time Profile] = dbo.GetTimeProfileString(@sDate, FLP.EndDate)
FROM	MRU_TMS_Liquidity_Reporting RPT
		INNER JOIN MRU_TMS_Deal_Override_FLP FLP ON RPT.[Deal No] = FLP.DealNo;

END

GO
/****** Object:  StoredProcedure [dbo].[MRU_TMS_TimeProfile_EndGrid_MakeTable]    Script Date: 2/11/2023 1:33:40 pm ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[MRU_TMS_TimeProfile_EndGrid_MakeTable]
	@sDate DATE
AS
BEGIN
	
SET NOCOUNT ON;

/********************************************************************************************************
 * The procedure populates MRU_TMS_TimeProfile_EndGrid which implements time profile definition of Analytics query
 * Definition method is a period and the grid definition is an end point.
 * That is, '3 Weeks' time profile string represents the period from the previous time profile (2 weeks, exclusive) to 3 weeks (inclusive).
 *
 ********************************************************************************************************
 *
 * @version 1.0
 * @author	Stephen Chin
 * @date	2021.01.15
 * 
 * - Initial version
 * 
 *
 *
*/

	DECLARE @1d date, @2d date, @3d date, @4d date, @5d date, @6d date;
	DECLARE @1w date, @2w date, @3w date;
	DECLARE @1ws date, @2ws date, @3ws date;
	DECLARE @1m date, @2m date;
	DECLARE @1ms date, @2ms date;
	DECLARE @1q date, @2q date, @3q date;
	DECLARE @1qs date, @2qs date, @3qs date;
	DECLARE @1y date, @2y date, @3y date, @4y date, @5y date, @10y date, @15y date, @20y date, @50y date;
	DECLARE @1ys date, @2ys date, @3ys date, @4ys date, @5ys date, @10ys date, @15ys date, @20ys date, @50ys date;

	SET @1d = DATEADD(DAY, 1, @sDate);
	SET @2d = DATEADD(DAY, 2, @sDate);
	SET @3d = DATEADD(DAY, 3, @sDate);
	SET @4d = DATEADD(DAY, 4, @sDate);
	SET @5d = DATEADD(DAY, 5, @sDate);
	SET @6d = DATEADD(DAY, 6, @sDate);

	SET @1ws = DATEADD(DAY, 1, @6d);
	SET @1w = DATEADD(WEEK, 1, @sDate);
	SET @2ws = DATEADD(DAY, 1, @1w);
	SET @2w = DATEADD(WEEK, 2, @sDate);
	SET @3ws = DATEADD(DAY, 1, @2w);
	SET @3w = DATEADD(WEEK, 3, @sDate);

	SET @1ms = DATEADD(DAY, 1, @3w);
	SET @1m = CASE WHEN @sdate = CONVERT(date, DATEADD(SS,-1,DATEADD(MM, DATEDIFF(MM,0,@sdate)+1,0)), 103) THEN DATEADD(SS, -1, DATEADD(MM, DATEDIFF(MM, 0, @sdate)+2, 0)) ELSE DATEADD(MONTH, 1, @sDate) END;
	SET @2ms = DATEADD(DAY, 1, @1m);
	SET @2m = CASE WHEN @sdate = CONVERT(date, DATEADD(SS,-1,DATEADD(MM, DATEDIFF(MM,0,@sdate)+1,0)), 103) THEN DATEADD(SS, -1, DATEADD(MM, DATEDIFF(MM, 0, @sdate)+3, 0)) ELSE DATEADD(MONTH, 2, @sDate) END;

	SET @1qs = DATEADD(DAY, 1, @2m);
	SET @1q = CASE WHEN @sdate = CONVERT(date, DATEADD(SS,-1,DATEADD(MM, DATEDIFF(MM,0,@sdate)+1,0)), 103) THEN DATEADD(SS, -1, DATEADD(MM, DATEDIFF(MM, 0, @sdate)+4, 0)) ELSE DATEADD(MONTH, 3, @sDate) END;
	SET @2qs = DATEADD(DAY, 1, @1q);
	SET @2q = CASE WHEN @sdate = CONVERT(date, DATEADD(SS,-1,DATEADD(MM, DATEDIFF(MM,0,@sdate)+1,0)), 103) THEN DATEADD(SS, -1, DATEADD(MM, DATEDIFF(MM, 0, @sdate)+7, 0)) ELSE DATEADD(MONTH, 6, @sDate) END;
	SET @3qs = DATEADD(DAY, 1, @2q);
	SET @3q = CASE WHEN @sdate = CONVERT(date, DATEADD(SS,-1,DATEADD(MM, DATEDIFF(MM,0,@sdate)+1,0)), 103) THEN DATEADD(SS, -1, DATEADD(MM, DATEDIFF(MM, 0, @sdate)+10, 0)) ELSE DATEADD(MONTH, 9, @sDate) END;

	SET @1ys = DATEADD(DAY, 1, @3q);
	SET @1y = CASE WHEN @sdate = CONVERT(date, DATEADD(SS,-1,DATEADD(MM, DATEDIFF(MM,0,@sdate)+1,0)), 103) THEN DATEADD(SS, -1, DATEADD(MM, DATEDIFF(MM, 0, @sdate)+13, 0)) ELSE DATEADD(YEAR, 1, @sDate) END;
	SET @2ys = DATEADD(DAY, 1, @1y);
	SET @2y = CASE WHEN @sdate = CONVERT(date, DATEADD(SS,-1,DATEADD(MM, DATEDIFF(MM,0,@sdate)+1,0)), 103) THEN DATEADD(SS, -1, DATEADD(MM, DATEDIFF(MM, 0, @sdate)+25, 0)) ELSE DATEADD(YEAR, 2, @sDate) END;
	SET @3ys = DATEADD(DAY, 1, @2y);
	SET @3y = CASE WHEN @sdate = CONVERT(date, DATEADD(SS,-1,DATEADD(MM, DATEDIFF(MM,0,@sdate)+1,0)), 103) THEN DATEADD(SS, -1, DATEADD(MM, DATEDIFF(MM, 0, @sdate)+37, 0)) ELSE DATEADD(YEAR, 3, @sDate) END;
	SET @4ys = DATEADD(DAY, 1, @3y);
	SET @4y = CASE WHEN @sdate = CONVERT(date, DATEADD(SS,-1,DATEADD(MM, DATEDIFF(MM,0,@sdate)+1,0)), 103) THEN DATEADD(SS, -1, DATEADD(MM, DATEDIFF(MM, 0, @sdate)+49, 0)) ELSE DATEADD(YEAR, 4, @sDate) END;
	SET @5ys = DATEADD(DAY, 1, @4y);
	SET @5y = CASE WHEN @sdate = CONVERT(date, DATEADD(SS,-1,DATEADD(MM, DATEDIFF(MM,0,@sdate)+1,0)), 103) THEN DATEADD(SS, -1, DATEADD(MM, DATEDIFF(MM, 0, @sdate)+61, 0)) ELSE DATEADD(YEAR, 5, @sDate) END;
	SET @10ys = DATEADD(DAY, 1, @5y);
	SET @10y = CASE WHEN @sdate = CONVERT(date, DATEADD(SS,-1,DATEADD(MM, DATEDIFF(MM,0,@sdate)+1,0)), 103) THEN DATEADD(SS, -1, DATEADD(MM, DATEDIFF(MM, 0, @sdate)+121, 0)) ELSE DATEADD(YEAR, 10, @sDate) END;
	SET @15ys = DATEADD(DAY, 1, @10y);
	SET @15y = CASE WHEN @sdate = CONVERT(date, DATEADD(SS,-1,DATEADD(MM, DATEDIFF(MM,0,@sdate)+1,0)), 103) THEN DATEADD(SS, -1, DATEADD(MM, DATEDIFF(MM, 0, @sdate)+181, 0)) ELSE DATEADD(YEAR, 15, @sDate) END;
	SET @20ys = DATEADD(DAY, 1, @15y);
	SET @20y = CASE WHEN @sdate = CONVERT(date, DATEADD(SS,-1,DATEADD(MM, DATEDIFF(MM,0,@sdate)+1,0)), 103) THEN DATEADD(SS, -1, DATEADD(MM, DATEDIFF(MM, 0, @sdate)+241, 0)) ELSE DATEADD(YEAR, 20, @sDate) END;
	SET @50ys = DATEADD(DAY, 1, @20y);
	SET @50y = CASE WHEN @sdate = CONVERT(date, DATEADD(SS,-1,DATEADD(MM, DATEDIFF(MM,0,@sdate)+1,0)), 103) THEN DATEADD(SS, -1, DATEADD(MM, DATEDIFF(MM, 0, @sdate)+601, 0)) ELSE DATEADD(YEAR, 50, @sDate) END;

	TRUNCATE TABLE MRU_TMS_TimeProfile_EndGrid;

	INSERT MRU_TMS_TimeProfile_EndGrid

	Select '1 Day', @1d, @1d		UNION ALL
	Select '2 Days', @2d, @2d		UNION ALL
	Select '3 Days', @3d, @3d		UNION ALL
	Select '4 Days', @4d, @4d		UNION ALL
	Select '5 Days', @5d, @5d		UNION ALL
	Select '6 Days', @6d, @6d		UNION ALL
	Select '1 Week', @1ws, @1w		UNION ALL
	Select '2 Weeks', @2ws, @2w		UNION ALL
	Select '3 Weeks', @3ws, @3w		UNION ALL
	Select '1 Month', @1ms, @1m		UNION ALL
	Select '2 Months', @2ms, @2m	UNION ALL
	Select '1 Quarter', @1qs, @1q	UNION ALL
	Select '2 Quarters', @2qs, @2q	UNION ALL
	Select '3 Quarters', @3qs, @3q	UNION ALL
	Select '1 Year', @1ys, @1y		UNION ALL
	Select '2 Years', @2ys, @2y		UNION ALL
	Select '3 Years', @3ys, @3y		UNION ALL
	Select '4 Years', @4ys, @4y		UNION ALL
	Select '5 Years', @5ys, @5y		UNION ALL
	Select '10 Years', @10ys, @10y	UNION ALL
	Select '15 Years', @15ys, @15y	UNION ALL
	Select '20 Years', @20ys, @20y	UNION ALL
	Select '50 Years', @50ys, @50y	;

END

GO
/****** Object:  StoredProcedure [dbo].[PROCESS_Liquidity_Data_Master]    Script Date: 2/11/2023 1:33:40 pm ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[PROCESS_Liquidity_Data_Master]

	(
	 @intReportNum	INT = 2
	,@intRunStyle	INT = 1
	)
AS

BEGIN
	
	/********************************************************************************************************
	*	PROCEDURE PURPOSE:                                                                                  *
	*	------------------                                                                                  *
	*                                                                                                       *
	*	Compiles all the procedures that were manually run in MS Access Pass-Through into one master        *
	*	procedure for the purposes of job scheduling the FALSH report                                       *
	*                                                                                                       *
	*  PROCEDURE PARAMETERS:                                                                                *
	*  ---------------------                                                                                *
	*                                                                                                       *
	*		Two	INPUT parameters                                                                            *
	*		Zero OUTPUT parameters                                                                          *
	*                                                                                                       *
	*	Parameter ReportNum is designed to cater different need,  For example, if Flash report is set to    *
	*	run, then MRU_Liquidity_RBNZ_PrivateReporting_MakeTable will be neglected. This reduces reduce run  *
	*	time but not compromise on the ratio accuracy in the morning.                                       *
	*                                                                                                       *
	*		@intReportNum :                                                                                 *
	*                                                                                                       *
	*           DEFAULTS to 2                                                                               *
	*                                                                                                       *
	*			1 to reset table [tblVariables]                                                             *
	*			2 for producing flash report data                                                           *
	*			3 for producing full report data                                                            *
	*                                                                                                       *
	*		@intRunStyle :                                                                                  *
	*                                                                                                       *
	*           DEFAULTS to 1                                                                               *	
	*                                                                                                       *
	*			1 process all execute statements                                                            *
	*			2 process only logging execute statements                                                   *
	*                                                                                                       *	
	*	FURTHER DETAIL:                                                                                     *
	*	---------------                                                                                     *
	*                                                                                                       *
	*	The complete Liquidity Data Processing Pipeline consists of all of the following procedures:		*
	*                                                                                                       *
	*	1.  EXECUTE MRU_AccountsBalanceJoint_MakeTable                                                      *
	*	2.  EXECUTE MRU_AccountsEOD_Update                                                                  *
	*                                                                                                       *
	*	3.  EXECUTE MRU_TMS_Liquidity_Reporting_Update                                                      *
	*	4.  EXECUTE MRU_TMS_Liquidity_Cashflows_Update                                                      *
	*                                                                                                       *
	*	5.  EXECUTE MRU_Retail_Card_MakeTable                                                               *
	*	6.  EXECUTE MRU_Liquidity_Funding_Balances_MakeTable                                                *
	*	7.  EXECUTE MRU_Client_Bandings_MakeTable                                                           *
	*                                                                                                       *
	*	8.  EXECUTE MRU_Liquidity_Funding_Balances_Retail_TD_MakeTable                                      *
	*	9.  EXECUTE MRU_Liquidity_Funding_Balances_Details_MakeTable                                        *
	*	10. EXECUTE MRU_Liquidity_Funding_Balances_Details_Bandings_MakeTable                               *
	*                                                                                                       *
	*	11. EXECUTE MRU_Liquidity_FBDB_ReassignedClientType_MakeTable_K62                                   *
	*	12. EXECUTE MRU_Liquidity_FBDB_ReassignedClientType_MakeTable_MissingANZSIC                         *
	*                                                                                                       *
	*	13. EXECUTE MRU_Pipeline_Business_MakeTable                                                         *
	*	14. EXECUTE MRU_Pipeline_Retail_MakeTable                                                           *
	*	15. EXECUTE MRU_Pipeline_Revolving_MakeTable                                                        *
	*                                                                                                       *
	*	16. EXECUTE MRU_Liquidity_RBNZ_DailyReporting_MakeTable                                             *
	*	17. EXECUTE MRU_Liquidity_RBNZ_PrivateReporting_MakeTable                                           *
	*                                                                                                       *
	*   18. EXECUTE MRU_Retail_Loan_Cashflows_MakeTable                                                     *
	*	19. EXECUTE MRU_Retail_Saving_Cashflows_MakeTable                                                   *
	*	20. EXECUTE MRU_Retail_TD_Cashflows_MakeTable                                                       *
	*	21. EXECUTE MRU_FO_PrimeSecondary_Deals_MakeTable                                                   *
	*	22. EXECUTE MRU_FO_Unsettled_Deals_MakeTable                                                        *
	*	23. EXECUTE MRU_FO_PrimeSecondary_Balances_MakeTable                                                *
	*	24. EXECUTE MRU_FO_Funding_Balances_MakeTable                                                       *
	*	25. EXECUTE MRU_FO_TMS_Cashflows_MakeTable                                                          *
	*	26. EXECUTE MRU_RBNZ_NewIssues_Balances_MakeTable                                                   *
	*	27. EXECUTE MRU_RBNZ_NewIssues_WAC_MakeTable                                                        *
	*                                                                                                       *
	*********************************************************************************************************
	*                                                                                                       *
	* @version	:	1.17 - System Change SQL  – Schedule Flash Report                                       *
	* @author	:	Shaun Barnarde, William Hsiao                                                                           *
	* @date		:	2023.07.07                                                                              *
	* @details	:   Initial version of PROCESS_Liquidity_Data_Master                                        * 
	*                                                                                                       * 
	*********************************************************************************************************
	*/

	/*  Apply standard configurations
	*/

	SET NOCOUNT ON;   

	/* Code start
	*/

	DECLARE @dEodDate			DATETIME
	DECLARE @dTimeStart			DATETIME
	DECLARE @dTimeEnd			DATETIME
	DECLARE @strUser			VARCHAR(255)
	DECLARE @strLogDescription	VARCHAR(255)
	DECLARE @strReport			VARCHAR(255)

	-- ------------------------------------------------------------------------------------------------------
	-- Assign report variables
	--

	-- current business date : TO DO, introduce date type error handler
	--

	SET @dEodDate = (
				SELECT TOP	1 
							DATEFROMPARTS(RIGHT(Variable,4), SUBSTRING (Variable, 4,2), LEFT(Variable, 2))
					FROM	tblVariables 
					WHERE	Description = 'ReportEndDate' 
					ORDER BY 1 DESC
					);


	-- current user 
	--
	SET @strUser = CURRENT_USER;
		
	-- ------------------------------------------------------------------------------------------------------
	-- based on @intReportNum, determine report type descriptor, this is required by process logging
	--

	SET @strReport = 
			CASE @intReportNum
				WHEN 1 THEN	'RESET VARIABLES'
				WHEN 2 THEN	'FLASH'
				WHEN 3 THEN	'FULL'
				ELSE 'UNKNOWN ' + CAST (@intReportNum as varchar(255))
				END;

	-- ------------------------------------------------------------------------------------------------------
	-- Based on @ReportNum, perform relevant processing 
	--
	-- Flash report (@intReportNum = 2), 
	--
	--		(a) Requires all of the pipeline to run, excluding the RBNZ liquidity Template funding details
	--		(b) Resets tblVariables
	--
	-- Full report (@intReportNum = 3), 
	--
	--		(a) Requires all of the pipeline to run
	--		(b) DOES NOT Reset tblVariables
	--
	-- Reset tblVariables (@intReportNum = 1), 
	--
	--		(a) Resets tblVariables
	--
	If (@intReportNum = 2) or (@intReportNum=3) 

		BEGIN

		/* ======================================================================================================
		** Funding Pipeline: 
		**
		** First part of the funding line, prepare balances, funding data, up until and including,
		** MRU_Liquidity_Funding_Balances_Details_Bandings
		** 
		** ======================================================================================================
		*/

			-- --------------------------------------------------------------------------------------------------
			-- MRU_AccountsBalanceJoint_MakeTable
			-- --------------------------------------------------------------------------------------------------
			--
			SET	@dTimeStart = GETDATE();						-- process start time
			
			IF (@intRunStyle <> 2) 
				BEGIN
				EXECUTE MRU_AccountsBalanceJoint_MakeTable;
				END;

			SET @dTimeEnd = GETDATE();							-- process end time

			-- Add the log entry. In time this PROCESS_LogAdd action will be embedded within the definition of 
			-- each of the data processing pipeline procedures.
			--
			SET @strLogDescription = @strReport + ':STEP_01A';
			EXECUTE PROCESS_LogAdd @dEodDate					-- End of day date
						, 'PROCESS_Liquidity_Data_Master' 		-- process
						, 'Run Data Processing Master' 			-- subprocess
						, 'MRU_AccountsBalanceJoint_MakeTable' 	-- procedure
						, @strLogDescription                    -- description
						, @strUser                              -- log user
						, @dTimeStart 							-- start time
						, @dTimeEnd								-- end time
						;

			-- --------------------------------------------------------------------------------------------------
			-- MRU_Accounts_EOD_Update
			--
			-- One could extend the procedure to cater for account balances reassignment as TRUE/FALSE and would 
			-- support forth coming ccontrol/check on balances
			--
			-- --------------------------------------------------------------------------------------------------
			--
			SET @dTimeStart = GETDATE();

			IF (@intRunStyle <> 2) 
				BEGIN
				EXECUTE MRU_AccountsEOD_Update;
				END;

			-- Add the log entry. 
			--
			SET	@dTimeEnd = GETDATE();
			SET @strLogDescription = @strReport  + ':STEP_01B'
			EXECUTE PROCESS_LogAdd @dEodDate
						, 'PROCESS_Liquidity_Data_Master'
						, 'Run Data Processing Master' 			
						, 'MRU_AccountsEOD_Update' 	
						, @strLogDescription, @strUser, @dTimeStart, @dTimeEnd;

			-- --------------------------------------------------------------------------------------------------
			-- MRU_TMS_Liquidity_Reporting_Update
			-- --------------------------------------------------------------------------------------------------
			--
			SET @dTimeStart = GETDATE();

			IF (@intRunStyle <> 2) 
				BEGIN
				EXECUTE MRU_TMS_Liquidity_Reporting_Update;
				END;

			-- Add the log entry. 
			--
			SET	@dTimeEnd = GETDATE();
			SET	@strLogDescription = @strReport  + ':STEP_01C';
			EXECUTE PROCESS_LogAdd @dEodDate
						, 'PROCESS_Liquidity_Data_Master'
						, 'Run Data Processing Master' 			
						, 'MRU_TMS_Liquidity_Reporting_Update' 	
						, @strLogDescription, @strUser, @dTimeStart, @dTimeEnd;

			-- --------------------------------------------------------------------------------------------------
			-- MRU_TMS_Liquidity_Cashflows_Update
			-- --------------------------------------------------------------------------------------------------
			--
			SET @dTimeStart = GETDATE();

			IF (@intRunStyle <> 2) 
				BEGIN
				EXECUTE MRU_TMS_Liquidity_Cashflows_Update;
				END;

			-- Add the log entry. 
			--
			SET	@dTimeEnd = GETDATE();
			SET	@strLogDescription = @strReport  + ':STEP_01D';
			EXECUTE PROCESS_LogAdd @dEodDate
						, 'PROCESS_Liquidity_Data_Master'
						, 'Run Data Processing Master' 			
						, 'MRU_TMS_Liquidity_Cashflows_Update' 	
						, @strLogDescription, @strUser, @dTimeStart, @dTimeEnd;

			-- --------------------------------------------------------------------------------------------------
			-- MRU_Retail_Card_MakeTable
			-- --------------------------------------------------------------------------------------------------
			--
			SET	@dTimeStart = GETDATE();

			IF (@intRunStyle <> 2) 
				BEGIN
				EXECUTE MRU_Retail_Card_MakeTable;
				END;

			-- Add the log entry. 
			--
			SET @dTimeEnd = GETDATE();
			SET @strLogDescription = @strReport  + ':STEP_01E';
			EXECUTE PROCESS_LogAdd @dEodDate
						, 'PROCESS_Liquidity_Data_Master'
						, 'Run Data Processing Master' 			
						, 'MRU_Retail_Card_MakeTable' 	
						, @strLogDescription, @strUser, @dTimeStart, @dTimeEnd;

			-- --------------------------------------------------------------------------------------------------
			-- MRU_Liquidity_Funding_Balances_MakeTable
			-- --------------------------------------------------------------------------------------------------
			--
			SET @dTimeStart = GETDATE();

			IF (@intRunStyle <> 2) 
				BEGIN
				EXECUTE MRU_Liquidity_Funding_Balances_MakeTable;
				END;

			-- Add the log entry. 
			--
			SET @dTimeEnd = GETDATE();
			SET @strLogDescription = @strReport  + ':STEP_01F';
			EXECUTE	PROCESS_LogAdd @dEodDate
						, 'PROCESS_Liquidity_Data_Master'
						, 'Run Data Processing Master' 			
						, 'MRU_Liquidity_Funding_Balances_MakeTable' 	
						, @strLogDescription, @strUser, @dTimeStart, @dTimeEnd;

			-- --------------------------------------------------------------------------------------------------
			-- MRU_Client_Bandings_MakeTable
			-- --------------------------------------------------------------------------------------------------
			--
			SET @dTimeStart = GETDATE();

			IF (@intRunStyle <> 2) 
				BEGIN
				EXECUTE MRU_Client_Bandings_MakeTable;
				END;

			-- Add the log entry. 
			--
			SET @dTimeEnd = GETDATE();
			SET @strLogDescription = @strReport  + ':STEP_01G';
			EXECUTE PROCESS_LogAdd @dEodDate
						, 'PROCESS_Liquidity_Data_Master'
						, 'Run Data Processing Master' 			
						, 'MRU_Client_Bandings_MakeTable' 	
						, @strLogDescription, @strUser, @dTimeStart, @dTimeEnd;

			-- --------------------------------------------------------------------------------------------------
			-- MRU_Liquidity_Funding_Balances_Retail_TD_MakeTable
			-- --------------------------------------------------------------------------------------------------
			--
			SET @dTimeStart = GETDATE();
			
			IF (@intRunStyle <> 2) 
				BEGIN
				EXECUTE MRU_Liquidity_Funding_Balances_Retail_TD_MakeTable;
				END;

			-- Add the log entry. 
			--
			SET @dTimeEnd = GETDATE();
			SET @strLogDescription = @strReport  + ':STEP_01H';
			EXECUTE PROCESS_LogAdd @dEodDate
						, 'PROCESS_Liquidity_Data_Master'
						, 'Run Data Processing Master' 			
						, 'MRU_Liquidity_Funding_Balances_Retail_TD_MakeTable' 	
						, @strLogDescription, @strUser, @dTimeStart, @dTimeEnd;

			-- --------------------------------------------------------------------------------------------------
			-- MRU_Liquidity_Funding_Balances_Details_MakeTable
			-- --------------------------------------------------------------------------------------------------
			--
			SET @dTimeStart = GETDATE();

			IF (@intRunStyle <> 2) 
				BEGIN
				EXECUTE MRU_Liquidity_Funding_Balances_Details_MakeTable;
				END;

			-- Add the log entry. 
			--
			SET @dTimeEnd = GETDATE();
			SET @strLogDescription = @strReport  + ':STEP_01I';
			EXECUTE PROCESS_LogAdd @dEodDate
						, 'PROCESS_Liquidity_Data_Master'
						, 'Run Data Processing Master' 			
						, 'MRU_Liquidity_Funding_Balances_Details_MakeTable' 	
						, @strLogDescription, @strUser, @dTimeStart, @dTimeEnd;

			-- --------------------------------------------------------------------------------------------------
			-- MRU_Liquidity_Funding_Balances_Details_Bandings_MakeTable
			-- --------------------------------------------------------------------------------------------------
			--
			SET @dTimeStart = GETDATE();

			IF (@intRunStyle <> 2) 
				BEGIN
				EXECUTE MRU_Liquidity_Funding_Balances_Details_Bandings_MakeTable;
				END;

			-- Add the log entry. 
			--
			SET @dTimeEnd = GETDATE();
			SET @strLogDescription = @strReport  + ':STEP_01J';
			EXECUTE PROCESS_LogAdd @dEodDate
						, 'PROCESS_Liquidity_Data_Master'
						, 'Run Data Processing Master' 			
						, 'MRU_Liquidity_Funding_Balances_Details_Bandings_MakeTable' 	
						, @strLogDescription, @strUser, @dTimeStart, @dTimeEnd;

		/* ******************************************************************************************************
		** Funding Pipeline: 
		**
		** Deal with missing anzsic & null client type 
		** 
		** ******************************************************************************************************
		*/

			-- ----------------------------------------------------------------------------------------
			-- Null  client type: MRU_Liquidity_FBDB_ReassignedClientType_MakeTable_K62
			-- ----------------------------------------------------------------------------------------
		
			SET @dTimeStart = GETDATE();

			IF (@intRunStyle <> 2) 
				BEGIN
				EXECUTE MRU_Liquidity_FBDB_ReassignedClientType_MakeTable_K62;
				END;

			-- Add the log entry. 
			--
			SET @dTimeEnd = GETDATE();
			SET @strLogDescription = @strReport  + ':STEP_02A';
			EXECUTE PROCESS_LogAdd @dEodDate
						, 'PROCESS_Liquidity_Data_Master'
						, 'Run Select and Update ClientType' 			
						, 'MRU_Liquidity_FBDB_ReassignedClientType_MakeTable_K62' 	
						, @strLogDescription, @strUser, @dTimeStart, @dTimeEnd;

			-- --------------------------------------------------------------------------------------------------
			-- missing ANZSIC : MRU_Liquidity_FBDB_ReassignedClientType_MakeTable_MissingANZSIC
			-- --------------------------------------------------------------------------------------------------

			SET @dTimeStart = GETDATE();

			IF (@intRunStyle <> 2) 
				BEGIN
				EXECUTE MRU_Liquidity_FBDB_ReassignedClientType_MakeTable_MissingANZSIC;
				END;

			-- Add the log entry. 
			--
			SET @dTimeEnd = GETDATE();
			SET @strLogDescription = @strReport  + ':STEP_02B';
			EXECUTE PROCESS_LogAdd @dEodDate
						, 'PROCESS_Liquidity_Data_Master'
						, 'Run Select and Update ClientType' 			
						, 'MRU_Liquidity_FBDB_ReassignedClientType_MakeTable_MissingANZSIC' 	
						, @strLogDescription, @strUser, @dTimeStart, @dTimeEnd;

		/* ******************************************************************************************************
		**
		** Reamining data processing pipeline elements and generating the RBNZ funding, 
		**
		** This is the third button in MS Access Liquidity.accdb "Run Retail Update Query with Log"
		** 
		** ******************************************************************************************************
		*/

			-- --------------------------------------------------------------------------------------------------
			-- pipeline business banking : MRU_Pipeline_Business_MakeTable
			-- --------------------------------------------------------------------------------------------------

			SET	@dTimeStart = GETDATE();

			IF (@intRunStyle <> 2) 
				BEGIN
				EXECUTE MRU_Pipeline_Business_MakeTable;
				END;

			-- Add the log entry. 
			--
			SET @dTimeEnd = GETDATE();
			SET @strLogDescription = @strReport  + ':STEP_03A';
			EXECUTE PROCESS_LogAdd @dEodDate
						, 'PROCESS_Liquidity_Data_Master'
						, 'Run Retail Update Query' 			
						, 'MRU_Pipeline_Business_MakeTable' 	
						, @strLogDescription, @strUser, @dTimeStart, @dTimeEnd;

			-- --------------------------------------------------------------------------------------------------
			-- pipeline retail : MRU_Pipeline_Retail_MakeTable
			-- --------------------------------------------------------------------------------------------------

			SET @dTimeStart = GETDATE();

			IF (@intRunStyle <> 2) 
				BEGIN
				EXECUTE MRU_Pipeline_Retail_MakeTable;
				END;

			-- Add the log entry. 
			--
			SET @dTimeEnd = GETDATE();
			SET @strLogDescription = @strReport  + ':STEP_03B';
			EXECUTE PROCESS_LogAdd @dEodDate
						, 'PROCESS_Liquidity_Data_Master'
						, 'Run Retail Update Query' 			
						, 'MRU_Pipeline_Retail_MakeTable' 	
						, @strLogDescription, @strUser, @dTimeStart, @dTimeEnd;
		
			-- --------------------------------------------------------------------------------------------------
			-- pipeline revolving : MRU_Pipeline_Revolving_MakeTable
			-- --------------------------------------------------------------------------------------------------

			SET	@dTimeStart = GETDATE();

			IF (@intRunStyle <> 2) 
				BEGIN
				EXECUTE MRU_Pipeline_Revolving_MakeTable;
				END;

			-- Add the log entry. 
			--
			SET	@dTimeEnd = GETDATE();
			SET	@strLogDescription = @strReport  + ':STEP_03C';
			EXECUTE PROCESS_LogAdd @dEodDate
						, 'PROCESS_Liquidity_Data_Master'
						, 'Run Retail Update Query' 			
						, 'MRU_Pipeline_Revolving_MakeTable' 	
						, @strLogDescription, @strUser, @dTimeStart, @dTimeEnd;

			-- --------------------------------------------------------------------------------------------------
			-- Funding final results: MRU_Liquidity_RBNZ_DailyReporting_MakeTable
			--
			-- for daily reporting and included in RBNZ metrics
			--
			-- --------------------------------------------------------------------------------------------------

			SET @dTimeStart = GETDATE();

			IF (@intRunStyle <> 2) 
				BEGIN
				EXECUTE MRU_Liquidity_RBNZ_DailyReporting_MakeTable;
				END;

			-- Add the log entry. 
			--
			SET @dTimeEnd = GETDATE();
			SET @strLogDescription = @strReport  + ':STEP_03D'
			EXECUTE PROCESS_LogAdd @dEodDate
						, 'PROCESS_Liquidity_Data_Master'
						, 'Run Retail Update Query' 			
						, 'MRU_Liquidity_RBNZ_DailyReporting_MakeTable' 	
						, @strLogDescription, @strUser, @dTimeStart, @dTimeEnd;

			-- --------------------------------------------------------------------------------------------------
			-- Funding final results: MRU_Liquidity_RBNZ_PrivateReporting_MakeTable
			--
			-- for monthly RBNZ liquidity template submission
			--
			-- as this procedure takes some time to run, we dont generate on a daily basis when runnning flash
			--
			-- --------------------------------------------------------------------------------------------------

			If	(@intReportNum=3)
			
				BEGIN

				SET @dTimeStart = GETDATE();

				IF (@intRunStyle <> 2) 
					BEGIN
					EXEC	MRU_Liquidity_RBNZ_PrivateReporting_MakeTable;
					END;

				-- Add the log entry. 
				--
				SET @dTimeEnd = GETDATE();
				SET @strLogDescription = @strReport  + ':STEP_03E';
				EXECUTE	PROCESS_LogAdd @dEodDate
							, 'PROCESS_Liquidity_Data_Master'
							, 'Run Retail Update Query' 			
							, 'MRU_Liquidity_RBNZ_PrivateReporting_MakeTable' 	
							, @strLogDescription, @strUser, @dTimeStart, @dTimeEnd;
				END

			-- --------------------------------------------------------------------------------------------------
			-- MRU_Retail_Loan_Cashflows_MakeTable
			-- --------------------------------------------------------------------------------------------------

			SET	@dTimeStart = GETDATE();

			IF (@intRunStyle <> 2) 
				BEGIN
				EXECUTE MRU_Retail_Loan_Cashflows_MakeTable;
				END;

			-- Add the log entry. 
			--
			SET @dTimeEnd = GETDATE();
			SET @strLogDescription = @strReport  + ':STEP_03F';
			EXECUTE PROCESS_LogAdd @dEodDate
						, 'PROCESS_Liquidity_Data_Master'
						, 'Run Retail Update Query' 			
						, 'MRU_Retail_Loan_Cashflows_MakeTable' 	
						, @strLogDescription, @strUser, @dTimeStart, @dTimeEnd;

			-- --------------------------------------------------------------------------------------------------
			-- MRU_Retail_Saving_Cashflows_MakeTable
			-- --------------------------------------------------------------------------------------------------

			SET	@dTimeStart = GETDATE();

			IF (@intRunStyle <> 2) 
				BEGIN
				EXECUTE MRU_Retail_Saving_Cashflows_MakeTable;
				END;

			-- Add the log entry. 
			--
			SET	@dTimeEnd = GETDATE();
			SET	@strLogDescription = @strReport  + ':STEP_03G';
			EXECUTE PROCESS_LogAdd @dEodDate
						, 'PROCESS_Liquidity_Data_Master'
						, 'Run Retail Update Query' 			
						, 'MRU_Retail_Saving_Cashflows_MakeTable' 	
						, @strLogDescription, @strUser, @dTimeStart, @dTimeEnd;

			-- --------------------------------------------------------------------------------------------------
			-- MRU_Retail_TD_Cashflows_MakeTable
			-- --------------------------------------------------------------------------------------------------
			
			SET	@dTimeStart = GETDATE();

			IF (@intRunStyle <> 2) 
				BEGIN
				EXECUTE MRU_Retail_TD_Cashflows_MakeTable;
				END;

			-- Add the log entry. 
			--
			SET	@dTimeEnd = GETDATE();
			SET	@strLogDescription = @strReport  + ':STEP_03H';
			EXECUTE PROCESS_LogAdd @dEodDate
						, 'PROCESS_Liquidity_Data_Master'
						, 'Run Retail Update Query' 			
						, 'MRU_Retail_TD_Cashflows_MakeTable' 	
						, @strLogDescription, @strUser, @dTimeStart, @dTimeEnd;

			-- --------------------------------------------------------------------------------------------------
			-- FRONT OFFICE, primary and secondary liquidity deals
			-- --------------------------------------------------------------------------------------------------
			
			SET	@dTimeStart = GETDATE();

			IF (@intRunStyle <> 2) 
				BEGIN
				EXECUTE MRU_FO_PrimeSecondary_Deals_MakeTable;
				END;

			-- Add the log entry. 
			--
			SET	@dTimeEnd = GETDATE();
			SET	@strLogDescription = @strReport  + ':STEP_03I';
			EXECUTE PROCESS_LogAdd @dEodDate
						, 'PROCESS_Liquidity_Data_Master'
						, 'Run Retail Update Query' 			
						, 'MRU_FO_PrimeSecondary_Deals_MakeTable' 	
						, @strLogDescription, @strUser, @dTimeStart, @dTimeEnd;

			-- --------------------------------------------------------------------------------------------------
			-- FRONT OFFICE, unsettled deals
			-- --------------------------------------------------------------------------------------------------

			SET	@dTimeStart = GETDATE();

			IF (@intRunStyle <> 2) 
				BEGIN
				EXECUTE MRU_FO_Unsettled_Deals_MakeTable;
				END;

			-- Add the log entry. 
			--
			SET	@dTimeEnd = GETDATE();
			SET	@strLogDescription = @strReport  + ':STEP_03J';
			EXECUTE PROCESS_LogAdd @dEodDate
						, 'PROCESS_Liquidity_Data_Master'
						, 'Run Retail Update Query' 			
						, 'MRU_FO_Unsettled_Deals_MakeTable' 	
						, @strLogDescription, @strUser, @dTimeStart, @dTimeEnd;

			-- --------------------------------------------------------------------------------------------------
			-- FRONT OFFICE, unsettled deals
			-- --------------------------------------------------------------------------------------------------

			SET	@dTimeStart = GETDATE();

			IF (@intRunStyle <> 2) 
				BEGIN
				EXECUTE MRU_FO_PrimeSecondary_Balances_MakeTable;
				END;

			-- Add the log entry. 
			--
			SET	@dTimeEnd = GETDATE();
			SET	@strLogDescription = @strReport  + ':STEP_03K';
			EXECUTE PROCESS_LogAdd @dEodDate
						, 'PROCESS_Liquidity_Data_Master'
						, 'Run Retail Update Query' 			
						, 'MRU_FO_PrimeSecondary_Balances_MakeTable' 	
						, @strLogDescription, @strUser, @dTimeStart, @dTimeEnd;

			-- --------------------------------------------------------------------------------------------------
			-- FRONT OFFICE, funding balances
			-- --------------------------------------------------------------------------------------------------

			SET	@dTimeStart = GETDATE();

			IF (@intRunStyle <> 2) 
				BEGIN
				EXECUTE MRU_FO_Funding_Balances_MakeTable;
				END;

			-- Add the log entry. 
			--
			SET	@dTimeEnd = GETDATE();
			SET	@strLogDescription = @strReport  + ':STEP_03L';
			EXECUTE PROCESS_LogAdd @dEodDate
						, 'PROCESS_Liquidity_Data_Master'
						, 'Run Retail Update Query' 			
						, 'MRU_FO_Funding_Balances_MakeTable' 	
						, @strLogDescription, @strUser, @dTimeStart, @dTimeEnd;
		
		
			-- --------------------------------------------------------------------------------------------------
			-- FRONT OFFICE, TMS Cashflows
			-- --------------------------------------------------------------------------------------------------
			
			SET	@dTimeStart = GETDATE();

			IF (@intRunStyle <> 2) 
				BEGIN
				EXECUTE MRU_FO_TMS_Cashflows_MakeTable;
				END;

			-- Add the log entry. 
			--
			SET	@dTimeEnd = GETDATE();
			SET	@strLogDescription = @strReport  + ':STEP_03M';
			EXECUTE PROCESS_LogAdd @dEodDate
						, 'PROCESS_Liquidity_Data_Master'
						, 'Run Retail Update Query' 			
						, 'MRU_FO_TMS_Cashflows_MakeTable' 	
						, @strLogDescription, @strUser, @dTimeStart, @dTimeEnd;

			-- --------------------------------------------------------------------------------------------------
			-- RBNZ, New Issues Balances
			-- --------------------------------------------------------------------------------------------------

			SET	@dTimeStart = GETDATE();

			IF (@intRunStyle <> 2) 
				BEGIN
				EXECUTE MRU_RBNZ_NewIssues_Balances_MakeTable;
				END;

			-- Add the log entry. 
			--
			SET	@dTimeEnd = GETDATE();
			SET	@strLogDescription = @strReport  + ':STEP_03N';
			EXECUTE PROCESS_LogAdd @dEodDate
						, 'PROCESS_Liquidity_Data_Master'
						, 'Run Retail Update Query' 			
						, 'MRU_RBNZ_NewIssues_Balances_MakeTable' 	
						, @strLogDescription, @strUser, @dTimeStart, @dTimeEnd;
		
			-- --------------------------------------------------------------------------------------------------
			-- RBNZ, New Issues, Weighted Average Cost
			-- --------------------------------------------------------------------------------------------------

			SET	@dTimeStart = GETDATE();

			IF (@intRunStyle <> 2) 
				BEGIN
				EXEC	MRU_RBNZ_NewIssues_WAC_MakeTable;
				END;

			-- Add the log entry. 
			--
			SET	@dTimeEnd = GETDATE();
			SET	@strLogDescription = @strReport  + ':STEP_03O';
			EXECUTE PROCESS_LogAdd @dEodDate
						, 'PROCESS_Liquidity_Data_Master'
						, 'Run Retail Update Query' 			
						, 'MRU_RBNZ_NewIssues_WAC_MakeTable' 	
						, @strLogDescription, @strUser, @dTimeStart, @dTimeEnd;

		END;

	IF (@intReportNum = 1) or (@intReportNum = 2)
		BEGIN
			
		-- Forth Button (Update Date to Today) 
		-- Side note: Email notification regarding TMS batch download will only be received once variables are reset. 

		SET	@dTimeStart = GETDATE();

		IF (@intRunStyle <> 2) 
			BEGIN
			UPDATE		tblVariables 
				SET		tblVariables.Variable = CONVERT (varchar, GETDATE(), 103)
				WHERE	tblVariables.Description = 'RetailProcessUnlockDate' 
					OR	tblVariables.Description = 'TMSProcessUnlockDate';
			END;

		-- Add the log entry. 
		--

		SET @dTimeEnd = GETDATE();
		SET	@strLogDescription = @strReport  + ':STEP_05A';
		EXECUTE PROCESS_LogAdd	@dEodDate
						, 'PROCESS_Liquidity_Data_Master'
						, 'Reset tblVariables' 			
						, 'Reset tblVariables' 	
						, @strLogDescription, @strUser, @dTimeStart, @dTimeEnd;

		END;

--EXEC msdb.dbo.sp_send_dbmail
--@profile_name = 'TMS',

--@body_format ='TEXT',

--@recipients = 'marketrisk@kiwibank.co.nz;*ittreasurysupport@kiwibank.co.nz', 

--@subject = 'PROCESS_Liquidity_Data_Master Completed...' ;

END
GO
/****** Object:  StoredProcedure [dbo].[PROCESS_LogAdd]    Script Date: 2/11/2023 1:33:40 pm ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[PROCESS_LogAdd]
	(
	 @EodDate			DATETIME
	,@strProcess		VARCHAR(255)
	,@strSubProcess		VARCHAR(255)
	,@strProcedure		VARCHAR(255)
	,@strDescription	VARCHAR(255)
	,@strUser			VARCHAR(255)	
	,@dTimeStart		DATETIME = GETDATE
	,@dTimeEnd			DATETIME = GETDATE
	)
AS
BEGIN

	/********************************************************************************************************
	*	PROCEDURE PURPOSE:                                                                                  *
	*	------------------                                                                                  *
	*                                                                                                       *
	*	(1) Liquidity Modelling process a mass of data. Some of these processes take some time to execute.  *
	*   (2) Recording process sequence and runtime to log table provides helpful process audit trail and    *
	*       privides data for tracking process performance.                                                 *
	*                                                                                                       *
	*  PROCEDURE PARAMETERS:                                                                                *
	*  ---------------------                                                                                *
	*                                                                                                       *
	*		Eight INPUT parameters                                                                          *
	*		Zero OUTPUT parameters                                                                          *
	*                                                                                                       *
	*	Notes on parameters:                                                                                *
	*                                                                                                       *
	*		None                                                                                            *
	*                                                                                                       *
	*	FURTHER DETAIL:                                                                                     *
	*	---------------                                                                                     *
	*                                                                                                       *
	*		None                                                                                            *
	*                                                                                                       *
	*********************************************************************************************************
	*                                                                                                       *
	* @version	:	1.17 - System Change SQL  – Schedule Flash Report                                       *
	* @author	:	Shaun Barnarde                                                                          *
	* @date		:	2023.07.17                                                                              *
	* @details	:   Initial version                                                                         * 
	*                                                                                                       * 
	*********************************************************************************************************
	*/

	/*  Apply standard configurations
	*/

	SET NOCOUNT ON;   

	/* Code start
	*/

	INSERT 
		INTO PROCESS_Log (
			logEodDate,
			logProcess,		logSubProcess,		logProcedure,	logDescription, 
			logUser,		logTimeStart,		logTimeEnd,		logCreatedUser,		logCreatedTime
		)
		VALUES (
			@EodDate,
			@strProcess,	@strSubProcess,		@strProcedure,	@strDescription, 
			@strUser,		@dTimeStart,		@dTimeEnd,		CURRENT_USER,		GETDATE()
		);
END;

GO
/****** Object:  StoredProcedure [dbo].[SBI365_Maketable]    Script Date: 2/11/2023 1:33:40 pm ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


CREATE PROCEDURE [dbo].[SBI365_Maketable]	
AS
BEGIN
	
SET NOCOUNT ON;

/********************************************************************************************************
 * This sums data for inwards and outwards of SBI365
 * Afterwards calculating Settle value and Cumulative_SettleValue by date and time
 *
 ********************************************************************************************************
 *
 * @version 1.0
 * @author	My Phan
 * @date	2023.10.05
 * 
 * - Start version control
 *
 *
*/


DELETE FROM SBI365_Sum;
INSERT	SBI365_Sum


Select	[Source], EodDate,
		cast(filedatetime_adj as date) as EODDate_adj,
		[Filename], [FileTime], [Filedatetime], [Filedatetime_adj], [TDR_Count], [TDR_Value], [TCR_Count], [TCR_Value], [FileType], Settle_Value,
		sum (Settle_value) over (PARTITION BY cast(filedatetime_adj as date) ORDER BY FileDateTime_adj) as Cumulative_SV

from

(select	'ISLinwards' as [Source],
		Group2 as EodDate,
		[Filename],
		[FileTime],
		cast(concat(Group2,' ',[FileTime]) as datetime) - '00:00' as filedatetime,
        cast(concat(Group2,' ',[FileTime]) as datetime) - '09:00' as filedatetime_adj,
		'In' as Filetype,
		[Total DR Count] as TDR_Count,
		[Total DR Value] as TDR_Value,
		[Total CR Count] as TCR_Count,
		[Total CR Value] as TCR_Value,
		[Total CR Value] - [Total DR Value] as Settle_Value
from	ISLinwards

Union all

select	'ISLOutwards' as [Source],
		Group2 as EodDate,
		[Filename],
		[FileTime],
		cast(concat(Group2,' ',[FileTime]) as datetime) - '00:00' as filedatetime,
        cast(concat(Group2,' ',[FileTime]) as datetime) - '09:00' as filedatetime_adj,
		'Out' as Filetype,
		[Total DR Count] as TDR_Count,
		[Total DR Value] as TDR_Value,
		[Total CR Count] as TCR_Count,
		[Total CR Value] as TCR_Value,
		[Total DR Value] - [Total CR Value] as Settle_Value
from	ISLOutwards) SBItmp

Order by	Filedatetime_adj


END

GO
/****** Object:  StoredProcedure [dbo].[Variables_CheckProcessLock]    Script Date: 2/11/2023 1:33:40 pm ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[Variables_CheckProcessLock]	
	@No INT,
	@ValueDate DATETIME
AS
BEGIN	
	SET NOCOUNT ON;

    SELECT CASE WHEN CONVERT(DATETIME, ISNULL(Variable, '1-Jan-1900'), 103) > @ValueDate THEN 1 ELSE 0 END Locked
    FROM tblVariables
    WHERE No = @No
END
GO
/****** Object:  StoredProcedure [dbo].[Variables_Select]    Script Date: 2/11/2023 1:33:40 pm ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[Variables_Select]
	
AS
BEGIN
	SET NOCOUNT ON;

	Select ReportEndDate, ReportStartDate, TMSProcessUnlockDate, RetailProcessUnlockDate
	FROM Variables
END
GO
/****** Object:  StoredProcedure [dbo].[Variables_UpdateReportDates]    Script Date: 2/11/2023 1:33:40 pm ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[Variables_UpdateReportDates]
	
AS
BEGIN
	
	SET NOCOUNT ON;
	
	DECLARE @ReportEndDate DATE = Calendar.dbo.AdvanceBusinessDays(getdate(), -1, 'NZ') 
	DECLARE @ReportStartDate DATE = DATEADD(month, DATEDIFF(month, 0, @ReportEndDate), 0);

    UPDATE tblVariables
    SET Variable = CONVERT(VARCHAR(10), @ReportEndDate, 103)
    WHERE No = 1
    
    UPDATE tblVariables
    SET Variable = CONVERT(VARCHAR(10), @ReportStartDate, 103)
    WHERE No = 2
    
END
GO
