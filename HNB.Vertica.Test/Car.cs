using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace HNB.Vertica.Test
{
    public class Car
    {
        public virtual int Id { get; set; }
        public virtual string Title { get; set; }
        public virtual string Description { get; set; }
        public virtual Maker Make { get; set; }
        public virtual Model Model { get; set; }
    }

    public class Maker
    {
        public virtual int Id { get; set; }
        public virtual string Name { get; set; }
        //public virtual IList<Model> Models { get; set; }
    }

    public class Model
    {
        public virtual int Id { get; set; }
        public virtual string Name { get; set; }
        public virtual Maker Make { get; set; }
    }

    public class Bus
    {
        public virtual int Id { get; set; }
        public virtual string Num { get; set; }
    }

    public class TestBulk
    {
        public virtual int Id { get; set; }
        public virtual string Name { get; set; }
        public virtual string IsOK { get; set; }
        public virtual double Score { get; set; }
        public virtual double Score1 { get; set; }
        public virtual double Score2 { get; set; }
    }

    public class Dic
    {
        public string dictName { get; set; }
        public string dictValue { get; set; }
        public string parentDictValue { get; set; }
        public string Memo { get; set; }
        public long? isValid { get; set; }
        public Boolean? isModify { get; set; }
        public DateTime? LoadTime { get; set; }
        public DateTime? etl_date { get; set; }
    }
}
