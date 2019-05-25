using FluentNHibernate.Mapping;

namespace HNB.Vertica.Test
{
    public class CarMap : ClassMap<Car>
    {
        public CarMap()
        {
            Id(x => x.Id).GeneratedBy.Assigned(); // Vertica doesn't have a way to return an sequence-generated identity
            Map(x => x.Title);
            Map(x => x.Description);
            References(x => x.Make).Column("MakeId");
            References(x => x.Model).Column("ModelId");
            Table("Car");
        }
    }

    public class MakeMap : ClassMap<Maker>
    {
        public MakeMap()
        {
            Id(x => x.Id).GeneratedBy.Assigned();
            Map(x => x.Name);
            //HasMany(x => x.Models)
            //    .KeyColumn("MakeId");
            Table("ods.Make");
        }
    }

    public class BusMap : ClassMap<Bus>
    {
        public BusMap()
        {
            Id(x => x.Id).GeneratedBy.Assigned();
            Map(x => x.Num);
            //HasMany(x => x.Models)
            //    .KeyColumn("MakeId");
            Table("ods.Bus");
        }
    }

    public class ModelMap : ClassMap<Model>
    {
        public ModelMap()
        {
            Id(x => x.Id).GeneratedBy.Assigned();
            Map(x => x.Name);
            References(x => x.Make)
                .Column("MakeId");
            Table("Model");
        }
    }

    public class TestBulkMap : ClassMap<TestBulk>
    {
        public TestBulkMap()
        {
            Id(x => x.Id).GeneratedBy.Assigned();
            Map(x => x.Name);
            Map(x => x.IsOK);
            Map(x => x.Score);
            Map(x => x.Score1);
            Map(x => x.Score2);
            Table("ods.TestBulk");
        }
    }
}
