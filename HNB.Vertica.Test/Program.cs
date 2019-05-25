using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NHibernate.Linq;
using System.Linq.Expressions;
using NHibernateVertica;
using System.Data;

namespace HNB.Vertica.Test
{
    class Program
    {
        static void Main(string[] args)
        {
            CarTest();
            Console.ReadLine();
        }

        private static void CarTest()
        {
            //Create
            //DataTable dt = new DataTable();
            //dt.Columns.Add("Id", typeof(int));
            //dt.Columns.Add("Name", typeof(string));
            //dt.Columns.Add("IsOK", typeof(string));
            //dt.Columns.Add("Score", typeof(double));
            //dt.Columns.Add("Score1", typeof(double));
            //dt.Columns.Add("Score2", typeof(double));

            //dt.Rows.Add(new object[] { 20000001, "test name", "Y", 191.123456789123456, 692.123456789123456, 993.123556789123456 });
            //dt.Rows.Add(new object[] { 20000002, "test name2", "Y", 194.123456789123456, 695.123456789123456, 996.123656789123456 });
            //dt.Rows.Add(new object[] { 20000003, "test name3", "N", 1910.123456789123456, 920.123456789123456, 930.123956789123456 });

            var dt = VerticaDBHelper.ExcelToDataTable<Dic>("C:\\Users\\kmuser\\Documents\\Desktop\\111.xls",false);

            VerticaDBHelper.BulkCopy<Dic>(dt, "dw.dw_global_dictionary");

            //Person magnus = new Person { Name = "Hedlund, Magnus" };
            //Person terry = new Person { Name = "Adams, Terry" };
            //Person charlotte = new Person { Name = "Weiss, Charlotte" };

            //Pet barley = new Pet { Name = "Barley", Owner = terry };
            //Pet boots = new Pet { Name = "Boots", Owner = terry };
            //Pet whiskers = new Pet { Name = "Whiskers", Owner = charlotte };
            //Pet daisy = new Pet { Name = "Daisy", Owner = magnus };

            //List<Person> people = new List<Person> { magnus, terry, charlotte };
            //List<Pet> pets = new List<Pet> { barley, boots, whiskers, daisy };

            //// Join the list of Person objects and the list of Pet objects 
            //// to create a list of person-pet pairs where each element is 
            //// an anonymous type that contains the name of pet and the name
            //// of the person that owns the pet.
            //var query = people.AsQueryable().Join(pets,
            //                person => person,
            //                pet => pet.Owner,
            //                (person, pet) =>
            //                    new { OwnerName = person.Name, Pet = pet.Name });

            //foreach (var obj in query)
            //{
            //    Console.WriteLine(
            //        "{0} - {1}",
            //        obj.OwnerName,
            //        obj.Pet);
            //}
        }

        class Person
        {
            public string Name { get; set; }
        }

        class Pet
        {
            public string Name { get; set; }
            public Person Owner { get; set; }
        }
    }
}
