using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FluentNHibernate.Cfg.Db;

namespace NHibernateVertica
{
    public class Vertica7ConnectionStringBuilder:ConnectionStringBuilder
    {
        private string _host;
        private int _port;
        private string _database;
        private string _username;
        private string _password;

        public Vertica7ConnectionStringBuilder Host(string host)
        {
            this._host = host;
            IsDirty = true;
            return this;
        }

        public Vertica7ConnectionStringBuilder Port(int port)
        {
            this._port = port;
            IsDirty = true;
            return this;
        }

        public Vertica7ConnectionStringBuilder Database(string database)
        {
            this._database = database;
            IsDirty = true;
            return this;
        }

        public Vertica7ConnectionStringBuilder Username(string username)
        {
            this._username = username;
            IsDirty = true;
            return this;
        }

        public Vertica7ConnectionStringBuilder Password(string password)
        {
            this._password = password;
            IsDirty = true;
            return this;
        }

        protected override string Create()
        {
            var connectionString = base.Create();
            if (!string.IsNullOrEmpty(connectionString))
                return connectionString;
            var sb = new StringBuilder();
            sb.AppendFormat("User Id={0};Password={1};Host={2};Port={3};Database={4};", _username, _password,
                _host, _port, _database);
            return sb.ToString();
        }
    }
}
