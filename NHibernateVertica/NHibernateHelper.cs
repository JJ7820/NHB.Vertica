using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FluentNHibernate.Cfg;
using FluentNHibernate.Cfg.Db;
using NHibernate;
using NHibernate.Tool.hbm2ddl;
using NHibernateVertica;

namespace NHibernateVertica
{
    public class NHibernateHelper<T>
    {
        private static ISessionFactory _sessionFactory;

        private static ISessionFactory SessionFactory
        {
            get
            {
                if (_sessionFactory == null)
                    InitializeSessionFactory();

                return _sessionFactory;
            }
        }

        private static void InitializeSessionFactory()
        {
            // InitializeSessionFactoryMySql();
            InitializeSessionFactoryVertica7();
        }

        /// <summary>
        /// test this MySQL as well
        /// </summary>
        private static void InitializeSessionFactoryMySql()
        {
            var cn = ConfigurationManager.ConnectionStrings["Vertica7"];

            _sessionFactory = Fluently.Configure()
                .Database(MsSqlConfiguration.MsSql2012
                              .ConnectionString(cn.ConnectionString)
                              .ShowSql()
                )
                .Mappings(m =>
                          m.FluentMappings
                              .AddFromAssemblyOf<T>())
                // comment this out to avoid schema autogeneration
                .ExposeConfiguration(cfg => new SchemaExport(cfg)
                                                .Create(true, true))

                /* consider controlling batch size for inserts and updates
				.ExposeConfiguration(config =>
				{
					config.SetProperty("adonet.batch_size", "1");
				})
				*/
                .BuildSessionFactory();
        }


        private static void InitializeSessionFactoryVertica7()
        {
            // TODO: fill in your credentials here!
            var cn = ConfigurationManager.ConnectionStrings["Vertica7"];
            _sessionFactory = Fluently.Configure()
                .Database(Vertica7Configuration.Standard
                              .ConnectionString(cn.ConnectionString)
                              .ShowSql()
                )
                .Mappings(m =>
                          m.FluentMappings
                              .AddFromAssemblyOf<T>())
                // have not yet needed the following in Vertica, though some say it may be worth modifying
                //.ExposeConfiguration()
                // enabling the following line will generate the schema
                //.ExposeConfiguration(cfg => new SchemaExport(cfg).Create(true, true))
                .BuildSessionFactory();
        }

        public static ISession OpenSession()
        {
            return SessionFactory.OpenSession();
        }
    }

}
