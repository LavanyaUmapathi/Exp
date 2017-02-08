USE [Stage_Three_DW]
GO

/****** Object:  Index [IDX_Monitor_Id_Current_Record]    Script Date: 10/18/2016 1:39:47 PM ******/
CREATE NONCLUSTERED INDEX [IDX_Monitor_Id_Current_Record] ON [dbo].[Dim_Monitor]
(
	[Monitor_ID] ASC,
	[Current_Record_Flag] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
GO

