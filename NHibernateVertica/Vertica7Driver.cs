using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace NHibernateVertica
{
    public class Vertica7Driver: NHibernate.Driver.ReflectionBasedDriver
    {
        public Vertica7Driver()
            : base("Vertica",
                  Assembly.LoadWithPartialName("Vertica.Data").FullName,
                  "Vertica.Data.VerticaClient.VerticaConnection",
                  "Vertica.Data.VerticaClient.VerticaCommand")
        {
        }

        public override bool UseNamedPrefixInSql => false;

        public override bool UseNamedPrefixInParameter => false;

        public override string NamedPrefix => string.Empty;

        public override bool SupportsMultipleOpenReaders => false;

        protected override bool SupportsPreparingCommands => true;

        public override bool SupportsMultipleQueries => true;

        public override NHibernate.Driver.IResultSetsCommand GetResultSetsCommand(NHibernate.Engine.ISessionImplementor session)
        {
            return new NHibernate.Driver.BasicResultSetsCommand(session);
        }

        protected override void InitializeParameter(IDbDataParameter dbParam, string name, NHibernate.SqlTypes.SqlType sqlType)
        {
            base.InitializeParameter(dbParam, name, sqlType);

            // Since the .NET currency type has 4 decimal places, we use a decimal type in PostgreSQL 
            // instead of its native 2 decimal currency type.
            if (sqlType.DbType == DbType.Currency)
                dbParam.DbType = DbType.Decimal;
        }
    }
}
