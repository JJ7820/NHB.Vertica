using NHibernate.Linq;
using NPOI.SS.UserModel;
using NPOI.XSSF.UserModel;
using NPOI.HSSF.UserModel;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using Vertica.Data.VerticaClient;


namespace NHibernateVertica
{
    public class VerticaDBHelper
    {
        /// <summary>
        /// 获取列表数据
        /// </summary>
        /// <typeparam name="T">实体类</typeparam>
        /// <param name="whereLambda">条件表达式</param>
        /// <returns></returns>
        public static IList<T> Read<T>(Expression<Func<T, bool>> whereLambda)
        {
            using (var session = NHibernateHelper<T>.OpenSession())
            {
                return session.Query<T>().Where(whereLambda).ToList();
            }
        }
        /// <summary>
        /// 获取列表数据并排序
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <typeparam name="TKey"></typeparam>
        /// <param name="whereLambda"></param>
        /// <param name="orderLambda"></param>
        /// <returns></returns>
        public static IList<T> Read<T,TKey>(Expression<Func<T, bool>> whereLambda, Expression<Func<T, TKey>> orderLambda)
        {
            using (var session = NHibernateHelper<T>.OpenSession())
            {
                return session.Query<T>().Where(whereLambda).OrderBy(orderLambda).ToList();
            }
        }
        /// <summary>
        /// 多表查询
        /// </summary>
        /// <typeparam name="TOuter">第一个序列中的元素的类型。</typeparam>
        /// <typeparam name="TInner">第二个序列中的元素的类型。</typeparam>
        /// <typeparam name="TKey">键选择器函数返回的键的类型。</typeparam>
        /// <typeparam name="TResult">结果元素的类型。</typeparam>
        /// <param name="whereLambda">条件表达式</param>
        /// <param name="inner">要与第一个序列联接的序列。</param>
        /// <param name="outerKeySelector">用于从第一个序列的每个元素提取联接键的函数。</param>
        /// <param name="innerKeySelector">用于从第二个序列的每个元素提取联接键的函数。</param>
        /// <param name="resultSelector">用于从两个匹配元素创建结果元素的函数。</param>
        /// <returns></returns>
        public static IList<TResult> Read<TOuter, TInner, TKey, TResult>(Expression<Func<TResult, bool>> whereLambda
            , IEnumerable<TInner> inner, Expression<Func<TOuter, TKey>> outerKeySelector
            , Expression<Func<TInner, TKey>> innerKeySelector, Expression<Func<TOuter, TInner, TResult>> resultSelector)
        {
            using (var session = NHibernateHelper<TOuter>.OpenSession())
            {
                return session.Query<TOuter>().Join(inner, outerKeySelector, innerKeySelector, resultSelector).Where(whereLambda).ToList();
            }
        }
        /// <summary>
        /// 获取首行首列数据
        /// </summary>
        /// <typeparam name="T">实体类</typeparam>
        /// <param name="whereLambda">条件表达式</param>
        /// <returns></returns>
        public static T ReadFirst<T>(Expression<Func<T, bool>> whereLambda)
        {
            using (var session = NHibernateHelper<T>.OpenSession())
            {
                return session.Query<T>().Where(whereLambda).First();
            }
        }
        /// <summary>
        /// 多表查询首行首列数据
        /// </summary>
        /// <typeparam name="TOuter">第一个序列中的元素的类型。</typeparam>
        /// <typeparam name="TInner">第二个序列中的元素的类型。</typeparam>
        /// <typeparam name="TKey">键选择器函数返回的键的类型。</typeparam>
        /// <typeparam name="TResult">结果元素的类型。</typeparam>
        /// <param name="whereLambda">条件表达式</param>
        /// <param name="inner">要与第一个序列联接的序列。</param>
        /// <param name="outerKeySelector">用于从第一个序列的每个元素提取联接键的函数。</param>
        /// <param name="innerKeySelector">用于从第二个序列的每个元素提取联接键的函数。</param>
        /// <param name="resultSelector">用于从两个匹配元素创建结果元素的函数。</param>
        /// <returns></returns>
        public static TResult ReadFirst<TOuter, TInner, TKey, TResult>(Expression<Func<TResult, bool>> whereLambda
            , IEnumerable<TInner> inner, Expression<Func<TOuter, TKey>> outerKeySelector
            , Expression<Func<TInner, TKey>> innerKeySelector, Expression<Func<TOuter, TInner, TResult>> resultSelector)
        {
            using (var session = NHibernateHelper<TOuter>.OpenSession())
            {
                return session.Query<TOuter>().Join(inner, outerKeySelector, innerKeySelector, resultSelector).Where(whereLambda).First();
            }
        }
        /// <summary>
        /// 分页查询
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <typeparam name="TKey"></typeparam>
        /// <param name="pageIndex"></param>
        /// <param name="pageSize"></param>
        /// <param name="whereLambda"></param>
        /// <param name="orderBy"></param>
        /// <returns></returns>
        public static IList<T> ReadByPage<T, TKey>(int pageIndex, int pageSize, Expression<Func<T, bool>> whereLambda, Expression<Func<T, TKey>> orderBy)
        {
            using (var session = NHibernateHelper<T>.OpenSession())
            {
                return session.Query<T>().Where(whereLambda).OrderBy(orderBy).Skip((pageIndex - 1) * pageSize).Take(pageSize).ToList();
            }
        }
        /// <summary>
        /// 单个删除
        /// </summary>
        /// <typeparam name="T">实体类</typeparam>
        /// <param name="t">实体</param>
        public static void Delete<T>(T t)
        {
            using (var session = NHibernateHelper<T>.OpenSession())
            {
                using (var transaction = session.BeginTransaction())
                {
                    try
                    {
                        session.Delete(t);
                        transaction.Commit();
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                        throw (ex);
                    }
                }
            }
        }
        /// <summary>
        /// 多个删除
        /// </summary>
        /// <typeparam name="T">实体类</typeparam>
        /// <param name="listTs">实体列表</param>
        public static void Delete<T>(IList<T> listTs)
        {
            using (var session = NHibernateHelper<T>.OpenSession())
            {
                using (var transaction = session.BeginTransaction())
                {
                    try
                    {
                        foreach (var t in listTs)
                        {
                            session.Delete(t);
                        }
                        transaction.Commit();
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                        throw (ex);
                    }
                }
            }
        }
        /// <summary>
        /// 单个更新
        /// </summary>
        /// <typeparam name="T">实体类</typeparam>
        /// <param name="t">实体</param>
        public static void Update<T>(T t)
        {
            using (var session = NHibernateHelper<T>.OpenSession())
            {
                using (var transaction = session.BeginTransaction())
                {
                    try
                    {
                        session.SaveOrUpdate(t);
                        transaction.Commit();
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                        throw (ex);
                    }
                }
            }
        }
        /// <summary>
        /// 多个更新
        /// </summary>
        /// <typeparam name="T">实体类</typeparam>
        /// <param name="listTs">实体列表</param>
        public static void Update<T>(IList<T> listTs)
        {
            using (var session = NHibernateHelper<T>.OpenSession())
            {
                using (var transaction = session.BeginTransaction())
                {
                    try
                    {
                        foreach (var t in listTs)
                        {
                            session.SaveOrUpdate(t);
                        }
                        transaction.Commit();
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                        throw (ex);
                    }
                }
            }
        }
        /// <summary>
        /// 单个新增
        /// </summary>
        /// <typeparam name="T">实体类</typeparam>
        /// <param name="t">实体</param>
        public static void Create<T>(T t)
        {
            using (var session = NHibernateHelper<T>.OpenSession())
            {
                using (var transaction = session.BeginTransaction())
                {
                    try
                    {
                        session.Save(t);
                        transaction.Commit();
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                        throw (ex);
                    }
                }
            }
        }
        /// <summary>
        /// 多个新增
        /// </summary>
        /// <typeparam name="T">实体类</typeparam>
        /// <param name="listTs">实体列表</param>
        public static void Create<T>(IList<T> listTs)
        {
            using (var session = NHibernateHelper<T>.OpenSession())
            {
                using (var transaction = session.BeginTransaction())
                {
                    try
                    {
                        foreach (var t in listTs)
                        {
                            session.Save(t);
                        }
                        transaction.Commit();
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                        throw (ex);
                    }
                }
            }
        }
        /// <summary>
        /// 数据复制
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="dt"></param>
        /// <param name="strTableName"></param>
        public static void BulkCopy<T>(DataTable dt, string strTableName)
        {
            MemoryStream ms = null;
            NHibernate.ITransaction txn = null;
            try
            {
                if (dt == null || dt.Rows.Count == 0)
                {
                    return;
                }
                if (dt.Columns.Count == 0)
                {
                    throw new Exception("The length of column cannot be zero.");
                }
                //从datatable中获取列名
                List<string> lstField = new List<string>();
                for (int colIndex = 0; colIndex < dt.Columns.Count; colIndex++)
                {
                    lstField.Add(dt.Columns[colIndex].ColumnName);
                }
                string strFiledList = string.Format("({0})", string.Join(",", lstField.ToArray()));
                //拼写copy语句
                const char RowSplit = '\n';
                const char ColSplit = '\t';

                string strCopyStatement = string.Format("copy {0}{1} from stdin record terminator E'{2}' delimiter E'{3}' enforcelength no commit",
                    strTableName, strFiledList, RowSplit, ColSplit);
                //按照copy语句中的分隔符，分隔数据源
                StringBuilder sbText = new StringBuilder();
                foreach (DataRow dr in dt.Rows)
                {
                    bool bFirstField = true;
                    for (int colIndex = 0; colIndex < dt.Columns.Count; colIndex++)
                    {
                        string strVal = GetDataString(dr, colIndex);
                        if (bFirstField)
                        {
                            sbText.Append(strVal);
                            bFirstField = false;
                        }
                        else
                        {
                            sbText.AppendFormat("{0}{1}", ColSplit, strVal);
                        }
                    }
                    sbText.Append(RowSplit);
                }
                //数据源写入内存流
                string strTemp = sbText.ToString();
                byte[] buff = Encoding.UTF8.GetBytes(strTemp);
                using (ms = new MemoryStream())
                {
                    ms.Write(buff, 0, buff.Length);
                    ms.Flush();
                    ms.Position = 0;
                    //建立vertica数据库连接
                    using (var session = NHibernateHelper<T>.OpenSession())
                    {
                        txn = session.BeginTransaction();
                        var vcs = new VerticaCopyStream((VerticaConnection)session.Connection, strCopyStatement);

                        vcs.Start();
                        vcs.AddStream(ms, false);
                        vcs.Execute();

                        long insertedCount = vcs.Finish();

                        IList<long> lstRejected = vcs.Rejects;
                        if (lstRejected.Count > 0)
                        {
                            txn.Rollback();
                            // Maybe need more detail info to show
                            throw new Exception("Bulk copy failure.");
                        }
                        else
                        {
                            txn.Commit();
                        }
                    }
                    ms.Close();
                }
            }
            catch(Exception ex)
            {
                txn.Rollback();
                ms.Close();
                throw (ex);
            }
        }

        private static string GetDataString(DataRow dr, int colIndex)
        {
            string strVal = "";
            if (!dr.IsNull(colIndex))
            {
                // only consider int/string
                strVal = dr[colIndex].ToString();
            }
            return strVal;
        }
        /// <summary>
        /// TXT转成DataTable
        /// </summary>
        /// <typeparam name="T">实体类</typeparam>
        /// <param name="pths">TXT的物理路径</param>
        /// <returns></returns>
        public static DataTable TxtToDataTable<T>(string pths)
        {
            StreamReader sr = null;
            try
            {
                using (sr = new StreamReader(pths, Encoding.GetEncoding("UTF-8")))
                {
                    string txt = sr.ReadToEnd().Replace("\r\n", "-");
                    string[] nodes = txt.Split('-');
                    Type type = typeof(T);
                    PropertyInfo[] properties = type.GetProperties();
                    DataTable dt = new DataTable(type.Name);
                    foreach (var propertie in properties)
                    {
                        dt.Columns.Add(new DataColumn(propertie.Name) { DataType = propertie.PropertyType });
                    }
                    foreach (string node in nodes)
                    {
                        string[] strs = node.Split('\t');
                        DataRow dr = dt.NewRow();
                        for (var i = 0; i < properties.Length; i++)
                        {
                            dr[properties[i].Name] = strs[i];
                        }
                        dt.Rows.Add(dr);
                    }
                    sr.Close();
                    return dt;
                }
            }
            catch (Exception ex)
            {
                sr.Close();
                throw (ex);
            }
        }
        /// <summary>
        /// Excel转成DataTable
        /// </summary>
        /// <typeparam name="T">实体类</typeparam>
        /// <param name="filePath">Excel文件物理地址</param>
        /// <param name="isColumnName">第一行是否是列名</param>
        /// <returns></returns>
        public static DataTable ExcelToDataTable<T>(string filePath, bool isColumnName)
        {
            DataTable dataTable = null;
            FileStream fs = null;
            DataRow dataRow = null;
            DataColumn column = null;
            IWorkbook workbook = null;
            ISheet sheet = null;
            IRow row = null;
            ICell cell = null;
            int startRow = 0;
            try
            {
                using (fs = File.OpenRead(filePath))
                {
                    // 2007版本  
                    if (filePath.IndexOf(".xlsx") > 0)
                        workbook = new XSSFWorkbook(fs);
                    // 2003版本  
                    else if (filePath.IndexOf(".xls") > 0)
                        workbook = new HSSFWorkbook(fs);

                    if (workbook != null)
                    {
                        sheet = workbook.GetSheetAt(0);//读取第一个sheet，当然也可以循环读取每个sheet  
                        dataTable = new DataTable();
                        if (sheet != null)
                        {
                            int rowCount = sheet.LastRowNum;//总行数  
                            if (rowCount > 0)
                            {
                                IRow firstRow = sheet.GetRow(0);//第一行  
                                int cellCount = firstRow.LastCellNum;//列数  
                                Type type = typeof(T);
                                PropertyInfo[] properties = type.GetProperties();
                                foreach (var propertie in properties)
                                {
                                    dataTable.Columns.Add(new DataColumn(propertie.Name) { DataType = propertie.PropertyType });
                                }
                                //构建datatable的列  
                                if (isColumnName)
                                {
                                    startRow = 1;//如果第一行是列名，则从第二行开始读取  
                                    //for (int i = firstRow.FirstCellNum; i < cellCount; ++i)
                                    //{
                                    //    cell = firstRow.GetCell(i);
                                    //    if (cell != null)
                                    //    {
                                    //        if (cell.StringCellValue != null)
                                    //        {
                                    //            column = new DataColumn(cell.StringCellValue);
                                    //            dataTable.Columns.Add(column);
                                    //        }
                                    //    }
                                    //}
                                }
                                //else
                                //{
                                //    for (int i = firstRow.FirstCellNum; i < cellCount; ++i)
                                //    {
                                //        column = new DataColumn("column" + (i + 1));
                                //        dataTable.Columns.Add(column);
                                //    }
                                //}

                                //填充行  
                                //for (int j = row.FirstCellNum; j < cellCount; ++j)
                                for (int i = startRow; i <= rowCount; ++i)
                                {
                                    row = sheet.GetRow(i);
                                    if (row == null) continue;

                                    dataRow = dataTable.NewRow();
                                    for (int j = 0; j < properties.Length; j++)
                                    {
                                        cell = row.GetCell(j);
                                        if (cell == null)
                                        {
                                            dataRow[properties[j].Name] = "";
                                        }
                                        else
                                        {
                                            //CellType(Unknown = -1,Numeric = 0,String = 1,Formula = 2,Blank = 3,Boolean = 4,Error = 5,)  
                                            switch (cell.CellType)
                                            {
                                                case CellType.Blank:
                                                    dataRow[properties[j].Name] = "";
                                                    break;
                                                case CellType.Numeric:
                                                    short format = cell.CellStyle.DataFormat;
                                                    //对时间格式（2015.12.5、2015/12/5、2015-12-5等）的处理  
                                                    if (format == 14 || format == 31 || format == 57 || format == 58)
                                                        dataRow[properties[j].Name] = cell.DateCellValue;
                                                    else
                                                        dataRow[properties[j].Name] = cell.NumericCellValue;
                                                    break;
                                                case CellType.String:
                                                    dataRow[properties[j].Name] = Encoding.UTF8.GetString(Encoding.UTF8.GetBytes(cell.StringCellValue));
                                                    break;
                                            }
                                        }
                                    }
                                    dataTable.Rows.Add(dataRow);
                                }
                            }
                        }
                    }
                    fs.Close();
                }
                return dataTable;
            }
            catch (Exception ex)
            {
                if (fs != null)
                {
                    fs.Close();
                }
                return null;
                throw (ex);
            }
        }
    }
}
