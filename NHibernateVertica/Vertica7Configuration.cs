using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FluentNHibernate.Cfg.Db;

namespace NHibernateVertica
{
    public class Vertica7Configuration :
        PersistenceConfiguration<Vertica7Configuration, Vertica7ConnectionStringBuilder>
    {
        protected Vertica7Configuration()
        {
            Driver<Vertica7Driver>();
        }

        public static Vertica7Configuration Standard => new Vertica7Configuration().Dialect<Vertica7Dialect>();
    }
}
