using System.Data;
using System.Data.Common;
using NHibernate;
using NHibernate.Dialect;
using NHibernate.Dialect.Function;
using NHibernate.Dialect.Schema;
using NHibernate.SqlCommand;
using NHibernate.SqlTypes;

namespace NHibernateVertica
{
    public class Vertica7Dialect: Dialect
    {
        public Vertica7Dialect()
        {
            DefaultProperties[NHibernate.Cfg.Environment.ConnectionDriver] = "NHibernate.Vertica.VerticaDriver";

            RegisterColumnType(DbType.AnsiStringFixedLength, "char(255)");
            RegisterColumnType(DbType.AnsiStringFixedLength, 8000, "char($l)");
            RegisterColumnType(DbType.AnsiString, "varchar(255)");
            RegisterColumnType(DbType.AnsiString, 65000, "varchar($l)");
            RegisterColumnType(DbType.AnsiString, 32000000, "long varchar($1)");
            RegisterColumnType(DbType.Binary, 65000, "varbinary");
            RegisterColumnType(DbType.Boolean, "boolean");
            RegisterColumnType(DbType.Byte, "tinyint");
            RegisterColumnType(DbType.Currency, "decimal(16,4)");
            RegisterColumnType(DbType.Date, "date");
            RegisterColumnType(DbType.DateTime, "timestamp");
            RegisterColumnType(DbType.Decimal, "decimal(19,5)");
            RegisterColumnType(DbType.Decimal, 19, "decimal($p, $s)");
            RegisterColumnType(DbType.Double, "float");
            RegisterColumnType(DbType.Int16, "smallint");
            RegisterColumnType(DbType.Int32, "int");
            RegisterColumnType(DbType.Int64, "bigint");
            RegisterColumnType(DbType.Single, "float");
            RegisterColumnType(DbType.StringFixedLength, "char(255)");
            RegisterColumnType(DbType.StringFixedLength, 65000, "char($l)");
            RegisterColumnType(DbType.String, "varchar(255)");
            RegisterColumnType(DbType.String, 4000, "varchar($l)");
            RegisterColumnType(DbType.String, 1073741823, "long varchar($1)");
            RegisterColumnType(DbType.Time, "time");

            // Override standard HQL function
            RegisterFunction("current_timestamp", new NoArgSQLFunction("now", NHibernateUtil.DateTime, true));
            RegisterFunction("str", new SQLFunctionTemplate(NHibernateUtil.String, "cast(?1 as varchar)"));
            RegisterFunction("locate", new PositionSubstringFunction());
            RegisterFunction("iif", new SQLFunctionTemplate(null, "case when ?1 then ?2 else ?3 end"));
            RegisterFunction("replace", new StandardSQLFunction("replace", NHibernateUtil.String));
            RegisterFunction("left", new SQLFunctionTemplate(NHibernateUtil.String, "substr(?1,1,?2)"));
            RegisterFunction("mod", new SQLFunctionTemplate(NHibernateUtil.Int32, "((?1) % (?2))"));
        }

        public override string AddColumnString => "add column";

        public override bool DropConstraints => false;

        public override string CascadeConstraintsString => " cascade";

        public override string GetSequenceNextValString(string sequenceName)
        {
            return string.Concat("select ", GetSelectSequenceNextValString(sequenceName));
        }

        public override string GetSelectSequenceNextValString(string sequenceName)
        {
            return string.Concat("nextval ('", sequenceName, "')");
        }

        public override string GetCreateSequenceString(string sequenceName)
        {
            return "create sequence " + sequenceName;
        }

        public override string GetDropSequenceString(string sequenceName)
        {
            return "drop sequence " + sequenceName;
        }

        /// <summary>
        /// override PostGresSQL behavior. Vertica does not support the RETURNING keyword
        /// </summary>
        /// <param name="insertString"></param>
        /// <param name="identifierColumnName"></param>
        /// <param name="parameterName"></param>
        /// <returns></returns>
        public override SqlString AddIdentifierOutParameterToInsert(SqlString insertString, string identifierColumnName, string parameterName)
        {
            return insertString;
        }

        public override InsertGeneratedIdentifierRetrievalMethod InsertGeneratedIdentifierRetrievalMethod => InsertGeneratedIdentifierRetrievalMethod.ReturnValueParameter;

        public override bool SupportsSequences => true;

        /// <summary>
        /// Supported with SQL 2003 syntax since 7.4, released 2003-11-17. For older versions
        /// we need to override GetCreateSequenceString(string, int, int) and provide alternative
        /// syntax, but I don't think we need to bother for such ancient releases (considered EOL).
        /// </summary>
        public override bool SupportsPooledSequences => true;

        public override bool SupportsLimit => true;

        public override bool SupportsLimitOffset => true;

        public override SqlString GetLimitString(SqlString queryString, SqlString offset, SqlString limit)
        {
            var pagingBuilder = new SqlStringBuilder();
            pagingBuilder.Add(queryString);

            if (limit != null)
            {
                pagingBuilder.Add(" limit ");
                pagingBuilder.Add(limit);
            }
            if (offset == null) return pagingBuilder.ToSqlString();
            pagingBuilder.Add(" offset ");
            pagingBuilder.Add(offset);

            return pagingBuilder.ToSqlString();
        }

        public override string GetForUpdateString(string aliases)
        {
            return ForUpdateString + " of " + aliases;
        }

        /// <summary>PostgreSQL supports UNION ALL clause</summary>
        /// <remarks>
        /// Reference: <see href="http://www.postgresql.org/docs/8.0/static/sql-select.html#SQL-UNION">
        /// PostgreSQL 8.0 UNION Clause documentation</see>
        /// </remarks>
        /// <value><see langword="true"/></value>
        public override bool SupportsUnionAll => true;

        /// <summary>PostgreSQL requires to cast NULL values to correctly handle UNION/UNION ALL</summary>
        /// <remarks>
        /// See <see href="http://archives.postgresql.org/pgsql-bugs/2005-08/msg00239.php">
        /// PostgreSQL BUG #1847: Error in some kind of UNION query.</see>
        /// </remarks>
        /// <param name="sqlType">The <see cref="DbType"/> type code.</param>
        /// <returns>null casted as <paramref name="sqlType"/>: "<c>null::sqltypename</c>"</returns>
        public override string GetSelectClauseNullString(SqlType sqlType)
        {
            //This will cast 'null' with the full SQL type name, including eventual parameters.
            //It shouldn't have any influence, but note that it's not mandatory.
            //i.e. 'null::decimal(19, 2)', even if 'null::decimal' would be enough
            return "null::" + GetTypeName(sqlType);
        }

        public override bool SupportsTemporaryTables => true;

        public override string CreateTemporaryTableString => "create temporary table";

        public override string CreateTemporaryTablePostfix => "on commit drop";

        public override string ToBooleanValueString(bool value)
        {
            return value ? "TRUE" : "FALSE";
        }

        public override string SelectGUIDString => "select uuid_generate_v4()";

        public override IDataBaseSchema GetDataBaseSchema(DbConnection connection)
        {
            return new PostgreSQLDataBaseMetadata(connection);
        }

        public override long TimestampResolutionInTicks => 10L;

        public override bool SupportsCurrentTimestampSelection => true;

        public override string CurrentTimestampSelectString => "SELECT CURRENT_TIMESTAMP";

        public override bool SupportsIdentityColumns => false;

        public override bool HasDataTypeInIdentityColumn => true;

        /// <summary>
        /// Unlike PostgreSQL Vertica does not support the SERIES keyword.
        /// </summary>
        /// <returns>Empty string</returns>
        public override string GetIdentityColumnString(DbType type)
        {
            return string.Empty;
        }

        /// <summary>
        /// The sql syntax to insert a row without specifying any column in PostgreSQL is
        /// <c>INSERT INTO table DEFAULT VALUES;</c>
        /// </summary>
        public override string NoColumnsInsertString => "default values";

        /// <summary>
        /// PostgreSQL 8.1 and above defined the fuction <c>lastval()</c> that returns the
        /// value of the last sequence that <c>nextval()</c> was used on in the current session.
        /// Call <c>lastval()</c> if <c>nextval()</c> has not yet been called in the current
        /// session throw an exception.
        /// </summary>
        public override string IdentitySelectString => "select currval('hibernate_sequence')";

        public override string IdentityInsertString => "nextval('hibernate_sequence')";

        public override SqlString AppendIdentitySelectToInsert(SqlString insertSql)
        {
            return insertSql.Append("; " + IdentitySelectString);
        }

        public override bool SupportsInsertSelectIdentity => true;
    }
}
