Commands to create the earnings call-related tables:

- CallCompany
    - create table CallCompany (Id uniqueidentifier primary key not null, Name varchar(255), TradingSymbol varchar(8))
- EarningsCall
    - create table EarningsCall (Id uniqueidentifier primary key not null, ForCompany uniqueidentifier, Url varchar(255), Title varchar(255), Period tinyint, Year smallint, HeldAt date)
- SpokenRemark
    - create table SpokenRemark (Id uniqueidentifier primary key not null, FromCall uniqueidentifier, SpokenBy uniqueidentifier, SequenceNumber smallint)
- CallParticipant
    - create table CallParticipant (Id uniqueidentifier primary key not null, Name varchar(255), Title varchar(255), IsExternal bit)
- SpokenRemarkHighlight
    - create table SpokenRemarkHighlight (Id uniqueidentifier primary key not null, SubjectRemark uniqueidentifier, BeginPosition smallint, EndPosition smallint, Category tinyint, Rating real)