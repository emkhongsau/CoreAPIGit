using System;
using System.Data;
using System.Data.SqlClient;
using System.Configuration;
using System.Collections;
using System.Text;

namespace CoreAPIGit.Common
{
	/// <summary>
	/// Summary description for CommonDB.
	/// </summary>
	public class Database
	{
		private string connectionString;
		private SqlConnection connection = null;
		private SqlTransaction transaction = null;


		/// <summary>
		/// Contructor 2.
		/// Su dung cac property database_ de xac lap thong tin ve database.
		/// </summary>
		public Database(string sqlCnnString)
		{
			this.connectionString = sqlCnnString;
		}
		public string SqlConnectionString
		{
			get
			{
				return this.connectionString;
			}
		}
		private bool debug = true;
		public bool Debug
		{
			get
			{
				return this.debug;
			}
			set
			{
				this.debug = value;
			}
		}
		/// <summary>
		/// CreateConnection
		/// </summary>
		/// <returns></returns>
		public SqlConnection CreateConnection()
		{
			return new SqlConnection(this.connectionString);
		}

		#region Transaction
		public void BeginTransaction()
		{
			if(this.connection == null)
			{
				this.connection = new SqlConnection(this.connectionString);
				this.connection.Open();
			}
			this.transaction = this.connection.BeginTransaction();
		}
		public void BeginTransaction(string transactionName)
		{
			if(this.connection == null)
			{
				this.connection = new SqlConnection(this.connectionString);
				this.connection.Open();
			}
			this.transaction = this.connection.BeginTransaction(transactionName);
		}
		public void BeginTransaction(IsolationLevel level)
		{
			if(this.connection == null)
			{
				this.connection = new SqlConnection(this.connectionString);
				this.connection.Open();
			}
			this.transaction = this.connection.BeginTransaction(level);
		}
		public void BeginTransaction(IsolationLevel level,string transactionName)
		{
			if(this.connection == null)
			{
				this.connection = new SqlConnection(this.connectionString);
				this.connection.Open();
			}
			this.transaction = this.connection.BeginTransaction(level,transactionName);
		}
		public void CommitTransaction()
		{
			if(this.transaction!=null)
			{
				this.transaction.Commit();
				this.transaction.Dispose();
				this.transaction = null;
			}
			if(this.connection!=null)
			{
				this.connection.Close();
				this.connection.Dispose();
				this.connection = null;
			}
		}
		public void RollbackTransaction()
		{
			if(this.transaction!=null)
			{
				this.transaction.Rollback();
				this.transaction.Dispose();
				this.transaction = null;
			}
			if(this.connection!=null)
			{
				this.connection.Close();
				this.connection.Dispose();
				this.connection = null;
			}
		}
		public void RollbackTransaction(string transactionName)
		{
			if(this.transaction!=null)
			{
				this.transaction.Rollback(transactionName);
				this.transaction.Dispose();
				this.transaction = null;
			}
			if(this.connection!=null)
			{
				this.connection.Close();
				this.connection.Dispose();
				this.connection = null;
			}
		}
        public void CloseConnection(SqlConnection con)
        {
            if (con != null)
            {
                con.Close();
                con.Dispose();
                con = null;
            }
        }

		#endregion Transaction

		#region Execute Query
		/// <summary>
		/// Thuc hien truy van database.
		/// </summary>
		/// <param name="strConnection">Chuoi connection tao boi phuong thuc CreateConnectionString.
		/// Dung de thuc hien ket noi database.</param>
		/// <param name="sqlSelect">Cau truy van.</param>
		/// <returns></returns>
		public static DataSet ExecuteQuery(string strConnection,string sqlSelect)
		{
			try
			{
				SqlDataAdapter adapter = new SqlDataAdapter(sqlSelect,strConnection);
				adapter.MissingSchemaAction = MissingSchemaAction.AddWithKey;
				DataSet ds = new DataSet();
				adapter.Fill(ds);
				adapter.Dispose();
				return ds;
			}
			catch(Exception ex)
			{
				throw ex;
			}
		}
		/// <summary>
		/// Thuc hien truy van database.
		/// </summary>
		/// <param name="strConnection">Chuoi connection tao boi phuong thuc CreateConnectionString.
		/// Dung de thuc hien ket noi database.</param>
		/// <param name="sqlSelect">Cau truy van.</param>
		/// <returns></returns>
		public DataSet ExecuteQuery(string sqlSelect)
		{
            SqlConnection con = this.connection;
            try
			{
				if(con==null)
				{
					con = new SqlConnection(this.connectionString);
					con.Open();
				}
				SqlDataAdapter adapter = new SqlDataAdapter(sqlSelect,con);
				adapter.MissingSchemaAction = MissingSchemaAction.AddWithKey;
				DataSet ds = new DataSet();
				adapter.Fill(ds);
				adapter.Dispose();
				if(this.connection==null)
					con.Close();

                this.CloseConnection(con);

				return ds;
			}
			catch(Exception ex)
			{
                this.CloseConnection(con);

				if(this.debug)	throw ex;
				return null;
			}
		}
		/// <summary>
		/// </summary>
		/// <param name="sql"></param>
		/// <param name="paramNames"></param>
		/// <param name="paramValues"></param>
		/// <returns></returns>
		public DataSet ExecuteQueryWithParam(string sql,string[] paramNames,object[] paramValues)
		{
			if(paramNames == null || paramValues == null)	return null;
			int N = Math.Min(paramNames.Length,paramValues.Length);
			if(N == 0)	return null;

            SqlConnection con = this.connection;
            try
			{
				if(con==null)
				{
					con = new SqlConnection(this.connectionString);
					con.Open();
				}
				SqlCommand cmd = new SqlCommand(sql,con);
				for(int i=0;i<N;i++)
				{
					SqlParameter param = new SqlParameter(Database.ProtectParamName(paramNames[i]),(paramValues[i]!=null) ? paramValues[i] : System.DBNull.Value);
					cmd.Parameters.Add(param);
				}
				SqlDataAdapter adapter = new SqlDataAdapter(cmd);
				adapter.MissingSchemaAction = MissingSchemaAction.AddWithKey;
				DataSet ds = new DataSet();
				adapter.Fill(ds);
				adapter.Dispose();
				cmd.Dispose();
				if(this.connection==null)
					con.Close();

                this.CloseConnection(con);

				return ds;
			}
			catch(Exception ex)
			{
                this.CloseConnection(con);

				if(this.debug)	throw ex;
				return null;
			}
		}
		/// <summary>
		/// </summary>
		/// <param name="strConnection">Chuoi connection tao boi phuong thuc CreateConnectionString.
		/// Dung de thuc hien ket noi database.</param>
		/// <param name="sqlSelect">Cau lenh SQL.</param>
		/// <returns></returns>
		public DataSet ExecuteQueryWithParam(string sql,string[] paramNames,SqlDbType[] paramTypes,object[] paramValues)
		{
			if(paramNames == null || paramTypes == null || paramValues == null)	return null;
			int N = Math.Min(paramNames.Length,paramTypes.Length);
			N = Math.Min(N,paramValues.Length);
			if(N == 0)	return null;

            SqlConnection con = this.connection;
			try
			{
				if(con==null)
				{
					con = new SqlConnection(this.connectionString);
					con.Open();
				}
				SqlCommand cmd = new SqlCommand(sql,con);
				for(int i=0;i<N;i++)
				{
					SqlParameter param = new SqlParameter(Database.ProtectParamName(paramNames[i]),paramTypes[i]);
					param.Value = (paramValues[i]!=null) ? paramValues[i] : System.DBNull.Value;
					cmd.Parameters.Add(param);
				}
				SqlDataAdapter adapter = new SqlDataAdapter(cmd);
				adapter.MissingSchemaAction = MissingSchemaAction.AddWithKey;
				DataSet ds = new DataSet();
				adapter.Fill(ds);
				adapter.Dispose();
				cmd.Dispose();
				if(this.connection==null)
					con.Close();

                this.CloseConnection(con);

				return ds;
			}
			catch(Exception ex)
			{
                this.CloseConnection(con);

				if(this.debug)	throw ex;
				return null;
			}
		}
		/// <summary>
		/// Thuc cau lenh database.
		/// </summary>
		/// <param name="strConnection">Chuoi connection tao boi phuong thuc CreateConnectionString.
		/// Dung de thuc hien ket noi database.</param>
		/// <param name="sqlSelect">Cau lenh SQL.</param>
		/// <returns></returns>
		public static int ExecuteNonQuery(string strConnection,string sqlSelect)
		{
            SqlConnection con = new SqlConnection(strConnection);

			try
			{
				con.Open();
				SqlCommand cmd = new SqlCommand(sqlSelect,con);
				int result = cmd.ExecuteNonQuery();
				cmd.Dispose();
				con.Close();
				return result;
			}
			catch(Exception ex)
			{
                if (con != null)
                {
                    con.Close();
                    con.Dispose();
                    con = null;
                }
				throw ex;
			}
		}
		/// <summary>
		/// Thuc cau lenh database.
		/// </summary>
		/// <param name="strConnection">Chuoi connection tao boi phuong thuc CreateConnectionString.
		/// Dung de thuc hien ket noi database.</param>
		/// <param name="sqlSelect">Cau lenh SQL.</param>
		/// <returns></returns>
		public int ExecuteNonQuery(string sqlSelect)
		{
            SqlConnection con = this.connection;
			try
			{
				if(con==null)
				{
					con = new SqlConnection(this.connectionString);
					con.Open();
				}
				SqlCommand cmd = new SqlCommand(sqlSelect,con);
				int result = cmd.ExecuteNonQuery();
				cmd.Dispose();
				if(this.connection==null)
					con.Close();

                this.CloseConnection(con);
				return result;
			}
			catch(Exception ex)
			{
                this.CloseConnection(con);

				if(this.debug)	throw ex;
				return -1;
			}
		}
		/// <summary>
		/// 
		/// </summary>
		/// <param name="sql"></param>
		/// <param name="paramNames"></param>
		/// <param name="paramValues"></param>
		/// <returns></returns>
		public int ExecuteNonQueryWithParam(string sql,string[] paramNames,object[] paramValues)
		{
			if(paramNames == null || paramValues == null)	return -1;
			int N = Math.Min(paramNames.Length,paramValues.Length);
			if(N == 0)	return 0;

            SqlConnection con = this.connection;
            try
			{
				if(con==null)
				{
					con = new SqlConnection(this.connectionString);
					con.Open();
				}
				SqlCommand cmd = new SqlCommand(sql,con);
				for(int i=0;i<N;i++)
				{
					SqlParameter param = new SqlParameter(Database.ProtectParamName(paramNames[i]),(paramValues[i]!=null) ? paramValues[i] : System.DBNull.Value);
					cmd.Parameters.Add(param);
				}
				int result = cmd.ExecuteNonQuery();
				cmd.Dispose();
				if(this.connection==null)
					con.Close();

                this.CloseConnection(con);
				return result;
			}
			catch(Exception ex)
			{
                this.CloseConnection(con);
				if(this.debug)	throw ex;
				return -1;
			}
		}
		/// <summary>
		/// ExecuteNonQueryWithParam.
		/// </summary>
		/// <param name="sql"></param>
		/// <param name="paramNames">Cau lenh sql co tham so(@).</param>
		/// <param name="paramTypes">Ten tham so(@sample).</param>
		/// <param name="paramValues">values.</param>
		public int ExecuteNonQueryWithParam(string sql,string[] paramNames,SqlDbType[] paramTypes,object[] paramValues)
		{
			if(paramNames == null || paramTypes == null || paramValues == null)
				throw new Exception("Database: Invalid parameter !");
			int N = Math.Min(paramNames.Length,paramTypes.Length);
			N = Math.Min(N,paramValues.Length);
			if(N == 0)
				throw new Exception("Database: Invalid parameter !");

            SqlConnection con = this.connection;
			try
			{
				if(con==null)
				{
					con = new SqlConnection(this.connectionString);
					con.Open();
				}
				SqlCommand cmd = new SqlCommand(sql,con);
				for(int i=0;i<N;i++)
				{
					SqlParameter param = new SqlParameter(Database.ProtectParamName(paramNames[i]),paramTypes[i]);
					param.Value = (paramValues[i]!=null) ? paramValues[i] : System.DBNull.Value;
					cmd.Parameters.Add(param);
				}
				int result = cmd.ExecuteNonQuery();
				cmd.Dispose();
				if(this.connection==null)
					con.Close();

                this.CloseConnection(con);

				return result;
			}
			catch(Exception ex)
			{
                this.CloseConnection(con);

				if(this.debug)	throw ex;
				return -1;
			}
		}
		/// <summary>
		/// 
		/// </summary>
		/// <param name="sql"></param>
		/// <param name="paramNames"></param>
		/// <param name="paramValues"></param>
		/// <returns></returns>
		public int ExecuteSerialNonQueryWithParam(string sql,string[] paramNames,ArrayList paramValues)
		{
			if(paramNames == null || paramValues == null
				|| paramNames.Length==0 || paramValues.Count==0)
				throw new Exception("Database: Invalid parameter !");

            SqlConnection con = this.connection;
			try
			{
				if(con==null)
				{
					con = new SqlConnection(this.connectionString);
					con.Open();
				}
				SqlCommand cmd = new SqlCommand(sql,con);
				for(int i=0;i<paramNames.Length;i++)
				{
					SqlParameter param = new SqlParameter();
					param.ParameterName = Database.ProtectParamName(paramNames[i]);
					cmd.Parameters.Add(param);
				}
				int count = 0,j;
				foreach(object[] values in paramValues)
				{
					j = 0;
					foreach(SqlParameter param in cmd.Parameters)
					{
						param.Value = (values[j]!=null) ? values[j] : System.DBNull.Value;
						j++;
					}
					count += cmd.ExecuteNonQuery();
				}
				cmd.Dispose();
				if(this.connection==null)
					con.Close();

                this.CloseConnection(con);
				return count;
			}
			catch(Exception ex)
			{
                this.CloseConnection(con);
				if(this.debug)	throw ex;
				return -1;
			}
		}
		/// <summary>
		/// ExecuteNonQueryWithParam.
		/// </summary>
		/// <param name="sql"></param>
		/// <param name="paramNames">Cau lenh sql co tham so(@).</param>
		/// <param name="paramTypes">Ten tham so(@sample).</param>
		/// <param name="paramValues">values.</param>
		public int ExecuteSerialNonQueryWithParam(string sql,string[] paramNames,SqlDbType[] paramTypes,ArrayList paramValues)
		{
			if(paramNames == null || paramTypes == null || paramValues == null)
				throw new Exception("Database: Invalid parameter !");
			int N = Math.Min(paramNames.Length,paramTypes.Length);
			if(N==0 || paramValues.Count==0)
				throw new Exception("Database: Invalid parameter !");


            SqlConnection con = this.connection;
			try
			{
				if(con==null)
				{
					con = new SqlConnection(this.connectionString);
					con.Open();
				}
				SqlCommand cmd = new SqlCommand(sql,con);
				for(int i=0;i<N;i++)
				{
					SqlParameter param = new SqlParameter(Database.ProtectParamName(paramNames[i]),paramTypes[i]);
					cmd.Parameters.Add(param);
				}
				int count = 0,j;
				foreach(object[] values in paramValues)
				{
					j = 0;
					foreach(SqlParameter param in cmd.Parameters)
					{
						param.Value = (values[j]!=null) ? values[j] : System.DBNull.Value;
						j++;
					}
					count += cmd.ExecuteNonQuery();
				}
				cmd.Dispose();
				if(this.connection==null)
					con.Close();

                this.CloseConnection(con);
				return count;
			}
			catch(Exception ex)
			{
                this.CloseConnection(con);
				if(this.debug)	throw ex;
				return -1;
			}
		}
		/// <summary>
		/// Thuc cau lenh database va tra ve gia tri dau tien.
		/// </summary>
		/// <param name="strConnection">Chuoi connection tao boi phuong thuc CreateConnectionString.
		/// Dung de thuc hien ket noi database.</param>
		/// <param name="sqlSelect">Cau lenh SQL.</param>
		/// <returns></returns>
		public static Object ExecuteScalar(string strConnection,string sqlSelect)
		{
            SqlConnection con = new SqlConnection(strConnection);
            try
			{
				con.Open();
				SqlCommand cmd = new SqlCommand(sqlSelect,con);
				Object result = cmd.ExecuteScalar();
				cmd.Dispose();
				con.Close();
				return result;
			}
			catch(Exception ex)
			{
                if (con != null)
                {
                    con.Close();
                    con.Dispose();
                    con = null;
                }
				throw ex;
			}
		}
		/// <summary>
		/// Thuc cau lenh database va tra ve gia tri dau tien.
		/// </summary>
		/// <param name="strConnection">Chuoi connection tao boi phuong thuc CreateConnectionString.
		/// Dung de thuc hien ket noi database.</param>
		/// <param name="sqlSelect">Cau lenh SQL.</param>
		/// <returns></returns>
		public object ExecuteScalar(string sqlSelect)
        {
            SqlConnection con = this.connection;
			try
			{
				if(con==null)
				{
					con = new SqlConnection(this.connectionString);
					con.Open();
				}
				SqlCommand cmd = new SqlCommand(sqlSelect,con);
				Object result = cmd.ExecuteScalar();
				cmd.Dispose();
				if(this.connection==null)
					con.Close();

                this.CloseConnection(con);
				return result;
			}
			catch(Exception ex)
			{
                this.CloseConnection(con);

				if(this.debug)	throw ex;
				return -1;
			}
		}
		/// <summary>
		/// 
		/// </summary>
		/// <param name="sql"></param>
		/// <param name="paramNames"></param>
		/// <param name="paramValues"></param>
		/// <returns></returns>
		public object ExecuteScalarWithParam(string sql,string[] paramNames,object[] paramValues)
		{
			if(paramNames == null || paramValues == null)	return false;
			int N = Math.Min(paramNames.Length,paramValues.Length);
			if(N == 0)	return false;

            SqlConnection con = this.connection;
			try
			{
				if(con==null)
				{
					con = new SqlConnection(this.connectionString);
					con.Open();
				}
				SqlCommand cmd = new SqlCommand(sql,con);
				for(int i=0;i<N;i++)
				{
					SqlParameter param = new SqlParameter(Database.ProtectParamName(paramNames[i]),(paramValues[i]!=null) ? paramValues[i] : System.DBNull.Value);
					cmd.Parameters.Add(param);
				}
				object rs = cmd.ExecuteScalar();
				cmd.Dispose();
				if(this.connection==null)
					con.Close();

                this.CloseConnection(con);
				return rs;
			}
			catch(Exception ex)
			{
                this.CloseConnection(con);

				if(this.debug)	throw ex;
				return -1;
			}
		}
		/// <summary>
		/// Thuc cau lenh database va tra ve gia tri dau tien.
		/// </summary>
		/// <param name="strConnection">Chuoi connection tao boi phuong thuc CreateConnectionString.
		/// Dung de thuc hien ket noi database.</param>
		/// <param name="sqlSelect">Cau lenh SQL.</param>
		/// <returns></returns>
		public object ExecuteScalarWithParam(string sql,string[] paramNames,SqlDbType[] paramTypes,object[] paramValues)
		{
			if(paramNames == null || paramTypes == null || paramValues == null)	return false;
			int N = Math.Min(paramNames.Length,paramTypes.Length);
			N = Math.Min(N,paramValues.Length);
			if(N == 0)	return false;

            SqlConnection con = this.connection;
			try
			{
				if(con==null)
				{
					con = new SqlConnection(this.connectionString);
					con.Open();
				}
				SqlCommand cmd = new SqlCommand(sql,con);
				for(int i=0;i<N;i++)
				{
					SqlParameter param = new SqlParameter(Database.ProtectParamName(paramNames[i]),paramTypes[i]);
					param.Value = (paramValues[i]!=null) ? paramValues[i] : System.DBNull.Value;
					cmd.Parameters.Add(param);
				}
				object rs = cmd.ExecuteScalar();
				cmd.Dispose();
				if(this.connection==null)
					con.Close();

                this.CloseConnection(con);
				return rs;
			}
			catch(Exception ex)
			{
                this.CloseConnection(con);

				if(this.debug)	throw ex;
				return -1;
			}
		}
		#endregion Execute Query

		#region Execute Store
        public int ExecuteSP(string storeName, string[] paramNames, object[] paramValues)
        {
            return this.ExecuteStoredProc(storeName, paramNames, paramValues, false);
        }

		public int ExecuteStoredProc(string storeName,Hashtable data)
		{
            return this.ExecuteStoredProc(storeName, data, false);
		}
		public int ExecuteStoredProc(string storeName,Hashtable data,bool checkSchema)
		{
            if (data == null || Functions.IsEmpty(storeName))
				return -1;

            SqlConnection con = this.connection;
			try
			{
				ArrayList schema = null;
				if(checkSchema)
				{
					schema = this.GetParamNamesArray(storeName);
					if(schema==null)
						throw new Exception("Cannot check schema !");
				}
				
				if(con==null)
				{
					con = new SqlConnection(this.connectionString);
					con.Open();
				}
				SqlCommand cmd = new SqlCommand(storeName,con);
				cmd.CommandType = CommandType.StoredProcedure;
				object value = null;
				int count = 0;
				foreach(string key in data.Keys)
				{
					if(checkSchema && !schema.Contains(Database.ProtectParamName(key)))
						continue;
					value = data[key];
					value = value!=null ? value : System.DBNull.Value;
					cmd.Parameters.Add(new SqlParameter(Database.ProtectParamName(key),value));
					count++;
				}
				if(checkSchema && count<schema.Count)
				{
					string tmp = string.Empty;
					foreach(string param in schema)
						if(!data.ContainsKey(param))
							tmp += param + ",";
					throw new Exception("Database Alert: Not enough parameter(" + tmp.Trim(new char[]{','}) + ") to execute store proc !");
				}
				SqlParameter returnParam = cmd.Parameters.Add("@RETURN_VALUE",SqlDbType.Int);
				returnParam.Direction = ParameterDirection.ReturnValue;
				cmd.ExecuteNonQuery();
				cmd.Dispose();
				if(this.connection==null)
					con.Close();

                this.CloseConnection(con);

				return (int)returnParam.Value;
			}
			catch(Exception ex)
			{
                this.CloseConnection(con);

				if(this.debug)	throw ex;
				return -1;
			}
		}
		public int ExecuteStoredProc(string storeName,string[] paramNames,object[] paramValues)
		{
			return this.ExecuteStoredProc(storeName,paramNames,paramValues,false);
		}
		public int ExecuteStoredProc(string storeName,string[] paramNames,object[] paramValues,bool checkSchema)
		{
            if (paramNames == null || paramValues == null || Functions.IsEmpty(storeName))
				return -1;
			int N = Math.Min(paramNames.Length,paramValues.Length);

            SqlConnection con = this.connection;
			try
			{
				ArrayList schema = null;
				if(checkSchema)
				{
					schema = this.GetParamNamesArray(storeName);
					if(schema==null)
						throw new Exception("Cannot check schema !");
				}

				if(con==null)
				{
					con = new SqlConnection(this.connectionString);
					con.Open();
				}
				SqlCommand cmd = new SqlCommand(storeName,con);
				cmd.CommandType = CommandType.StoredProcedure;
				int count = 0;
				for(int i=0;i<N;i++)
				{
					if(checkSchema && !schema.Contains(Database.ProtectParamName(paramNames[i])))
						continue;
					SqlParameter param = new SqlParameter(Database.ProtectParamName(paramNames[i]),(paramValues[i]!=null) ? paramValues[i] : System.DBNull.Value);
					cmd.Parameters.Add(param);
					count++;
				}
				if(checkSchema && count<schema.Count)
				{
					string tmp = string.Empty;
					ArrayList tmpArray = new ArrayList(paramNames);
					foreach(string param in schema)
						if(!tmpArray.Contains(param))
							tmp += param + ",";
					throw new Exception("Database Alert: Not enough parameter(" + tmp.Trim(new char[]{','}) + ") to execute store proc !");
				}
				SqlParameter returnParam = cmd.Parameters.Add("@RETURN_VALUE",SqlDbType.Int);
				returnParam.Direction = ParameterDirection.ReturnValue;
				cmd.ExecuteNonQuery();
				cmd.Dispose();
				if(this.connection==null)
					con.Close();

                this.CloseConnection(con);

				return (int)returnParam.Value;
			}
			catch(Exception ex)
			{
                this.CloseConnection(con);

				if(this.debug)	throw ex;
				return -1;
			}
		}
		public int ExecuteStoredProc(string storeName,string[] paramNames,SqlDbType[] paramTypes,object[] paramValues)
		{
            return this.ExecuteStoredProc(storeName, paramNames, paramTypes, paramValues, false);
		}
		public int ExecuteStoredProc(string storeName,string[] paramNames,SqlDbType[] paramTypes,object[] paramValues,bool checkSchema)
		{
            if (paramNames == null || paramTypes == null || paramValues == null || Functions.IsEmpty(storeName))
				return -1;
			int N = Math.Min(paramNames.Length,paramTypes.Length);
			N = Math.Min(N,paramValues.Length);

            SqlConnection con = this.connection;
			try
			{
				ArrayList schema = null;
				if(checkSchema)
				{
					schema = this.GetParamNamesArray(storeName);
					if(schema==null)
						throw new Exception("Cannot check schema !");
				}

				if(con==null)
				{
					con = new SqlConnection(this.connectionString);
					con.Open();
				}
				SqlCommand cmd = new SqlCommand(storeName,con);
				cmd.CommandType = CommandType.StoredProcedure;
				int count=0;
				for(int i=0;i<N;i++)
				{
					if(checkSchema && !schema.Contains(Database.ProtectParamName(paramNames[i])))
						continue;
					SqlParameter param = new SqlParameter(Database.ProtectParamName(paramNames[i]),paramTypes[i]);
					param.Value = (paramValues[i]!=null) ? paramValues[i] : System.DBNull.Value;
					cmd.Parameters.Add(param);
					count++;
				}
				if(checkSchema && count<schema.Count)
				{
					string tmp = string.Empty;
					ArrayList tmpArray = new ArrayList(paramNames);
					foreach(string param in schema)
						if(!tmpArray.Contains(param))
							tmp += param + ",";
					throw new Exception("Database Alert: Not enough parameter(" + tmp.Trim(new char[]{','}) + ") to execute store proc !");
				}
				SqlParameter returnParam = cmd.Parameters.Add("@RETURN_VALUE",SqlDbType.Int);
				returnParam.Direction = ParameterDirection.ReturnValue;
				int result = cmd.ExecuteNonQuery();
				cmd.Dispose();
				if(this.connection==null)
					con.Close();

                this.CloseConnection(con);

				return (int)returnParam.Value;
			}
			catch(Exception ex)
			{
                this.CloseConnection(con);

				if(this.debug)	throw ex;
				return -1;
			}
		}
		public DataSet ExecuteStoredProcReturnDataSet(string storeName,Hashtable data)
		{
            return this.ExecuteStoredProcReturnDataSet(storeName, data, false);
		}
		public DataSet ExecuteStoredProcReturnDataSet(string storeName,Hashtable data,out int returnValue)
		{
            return this.ExecuteStoredProcReturnDataSet(storeName, data, out returnValue, false);
		}
		public DataSet ExecuteStoredProcReturnDataSet(string storeName,Hashtable data,bool checkSchema)
		{
			int returnValue;
			return this.ExecuteStoredProcReturnDataSet(storeName,data,out returnValue,checkSchema);
		}
		public DataSet ExecuteStoredProcReturnDataSet(string storeName,Hashtable data,out int returnValue,bool checkSchema)
		{
            if (data == null || Functions.IsEmpty(storeName))
				throw new Exception("Invalid Argument");

            SqlConnection con = this.connection;
			try
			{
				ArrayList schema = null;
				if(checkSchema)
				{
					schema = this.GetParamNamesArray(storeName);
					if(schema==null)
						throw new Exception("Cannot check schema !");
				}

				if(con==null)
				{
					con = new SqlConnection(this.connectionString);
					con.Open();
				}
				SqlCommand cmd = new SqlCommand(storeName,con);
				cmd.CommandType = CommandType.StoredProcedure;
				object value = null;
				int count = 0;
				foreach(string key in data.Keys)
				{
					if(checkSchema && !schema.Contains(Database.ProtectParamName(key)))
						continue;
					value = data[key];
					value = value!=null ? value : System.DBNull.Value;
					cmd.Parameters.Add(new SqlParameter(Database.ProtectParamName(key),value));
					count++;
				}
				if(checkSchema && count<schema.Count)
				{
					string tmp = string.Empty;
					foreach(string param in schema)
						if(!data.ContainsKey(param))
							tmp += param + ",";
					throw new Exception("Database Alert: Not enough parameter(" + tmp.Trim(new char[]{','}) + ") to execute store proc !");
				}
				SqlParameter returnParam = cmd.Parameters.Add("@RETURN_VALUE",SqlDbType.Int);
				returnParam.Direction = ParameterDirection.ReturnValue;
				SqlDataAdapter adapter = new SqlDataAdapter(cmd);
				DataSet ds = new DataSet();
				adapter.Fill(ds);
				adapter.Dispose();
				cmd.Dispose();
				if(this.connection==null)
					con.Close();

                this.CloseConnection(con);

				returnValue = (int)returnParam.Value;
				return ds;
			}
			catch(Exception ex)
			{
                this.CloseConnection(con);

				if(this.debug)	throw ex;
				returnValue = -1;
				return null;
			}
		}
        public DataSet ExecuteSPReturnDS(string sql, string[] paramNames, object[] paramValues)
        {
            return this.ExecuteStoredProcReturnDataSet(sql, paramNames, paramValues, false);
        }
        public DataSet ExecuteSPReturnDS_Value(string sql, string[] paramNames, object[] paramValues, out int returnValue)
        {
            return this.ExecuteStoredProcReturnDataSet(sql, paramNames, paramValues, out returnValue, false);
        }
		public DataSet ExecuteSPReturnDS_Value_2(string sql, string[] paramNames, object[] paramValues, out int returnValue)
		{
			return this.ExecuteStoredProcReturnDataSet2(sql, paramNames, paramValues, out returnValue, false);
		}

		public DataSet ExecuteStoredProcReturnDataSet(string sql,string[] paramNames,object[] paramValues)
		{
			return this.ExecuteStoredProcReturnDataSet(sql,paramNames,paramValues,false);
		}
		public DataSet ExecuteStoredProcReturnDataSet(string sql,string[] paramNames,object[] paramValues,out int returnValue)
		{
			return this.ExecuteStoredProcReturnDataSet(sql,paramNames,paramValues,out returnValue,false);
		}
		public DataSet ExecuteStoredProcReturnDataSet(string storeName,string[] paramNames,object[] paramValues,bool checkSchema)
		{
			int returnValue;
			return this.ExecuteStoredProcReturnDataSet(storeName,paramNames,paramValues,out returnValue,checkSchema);
		}
		
		public DataSet ExecuteStoredProcReturnDataSet2(string storeName, string[] paramNames, object[] paramValues, out int returnValue, bool checkSchema)
		{
			returnValue = -1;
			if (paramNames == null || paramValues == null) return null;
			int N = Math.Min(paramNames.Length, paramValues.Length);

			SqlConnection con = this.connection;
			try
			{
				ArrayList schema = null;
				if (checkSchema)
				{
					schema = this.GetParamNamesArray(storeName);
					if (schema == null)
						throw new Exception("Cannot check schema !");
				}
				con = new SqlConnection(this.connectionString);
				con.Open();
				//if(con==null)
				//{
				//	con = new SqlConnection(this.connectionString);
				//	con.Open();
				//}
				SqlCommand cmd = new SqlCommand(storeName, con);
				cmd.CommandType = CommandType.StoredProcedure;
				cmd.CommandTimeout = 60 * 5;
				int count = 0;
				for (int i = 0; i < N; i++)
				{
					if (checkSchema && !schema.Contains(Database.ProtectParamName(paramNames[i])))
						continue;
					SqlParameter param = new SqlParameter(Database.ProtectParamName(paramNames[i]), (paramValues[i] != null) ? paramValues[i] : System.DBNull.Value);
					cmd.Parameters.Add(param);
					count++;
				}
				if (checkSchema && count < schema.Count)
				{
					string tmp = string.Empty;
					ArrayList tmpArray = new ArrayList(paramNames);
					foreach (string param in schema)
						if (!tmpArray.Contains(param))
							tmp += param + ",";
					//throw new Exception("Database Alert: Not enough parameter(" + tmp.Trim(new char[]{','}) + ") to execute store proc !");
				}
				SqlParameter returnParam = cmd.Parameters.Add("@RETURN_VALUE", SqlDbType.Int);
				returnParam.Direction = ParameterDirection.ReturnValue;
				SqlDataAdapter adapter = new SqlDataAdapter(cmd);
				DataSet ds = new DataSet();
				adapter.Fill(ds);
				adapter.Dispose();
				cmd.Dispose();
				if (this.connection == null)
					con.Close();

				this.CloseConnection(con);
				returnValue = (int)returnParam.Value;
				return ds;
			}
			catch (Exception ex)
			{
				//Functions.WriteLogException($"Call [{storeName}] [Exception]:" + ex.ToString());
				this.CloseConnection(con);

				//if(this.debug)	throw ex;
				return null;
			}
		}
		public DataSet ExecuteStoredProcReturnDataSet(string storeName,string[] paramNames,object[] paramValues,out int returnValue,bool checkSchema)
		{
			returnValue = -1;
			if(paramNames == null || paramValues == null)	return null;
			int N = Math.Min(paramNames.Length,paramValues.Length);

            SqlConnection con = this.connection;
			try
			{
				ArrayList schema = null;
				if(checkSchema)
				{
					schema = this.GetParamNamesArray(storeName);
					if(schema==null)
						throw new Exception("Cannot check schema !");
				}
				con = new SqlConnection(this.connectionString);
				con.Open();
				if(con==null)
				{
					con = new SqlConnection(this.connectionString);
					con.Open();
				}
				SqlCommand cmd = new SqlCommand(storeName,con);
				cmd.CommandType = CommandType.StoredProcedure;
				cmd.CommandTimeout = 60 * 5;
				int count = 0;
				for(int i=0;i<N;i++)
				{
					if(checkSchema && !schema.Contains(Database.ProtectParamName(paramNames[i])))
						continue;
					SqlParameter param = new SqlParameter(Database.ProtectParamName(paramNames[i]),(paramValues[i]!=null) ? paramValues[i] : System.DBNull.Value);
					cmd.Parameters.Add(param);
					count++;
				}
				if(checkSchema && count<schema.Count)
				{
					string tmp = string.Empty;
					ArrayList tmpArray = new ArrayList(paramNames);
					foreach(string param in schema)
						if(!tmpArray.Contains(param))
							tmp += param + ",";
					//throw new Exception("Database Alert: Not enough parameter(" + tmp.Trim(new char[]{','}) + ") to execute store proc !");
				}
				SqlParameter returnParam = cmd.Parameters.Add("@RETURN_VALUE",SqlDbType.Int);
				returnParam.Direction = ParameterDirection.ReturnValue;
				SqlDataAdapter adapter = new SqlDataAdapter(cmd);
				DataSet ds = new DataSet();
				adapter.Fill(ds);
				adapter.Dispose();
				cmd.Dispose();
				if(this.connection==null)
					con.Close();

                this.CloseConnection(con);
				returnValue = (int)returnParam.Value;
				return ds;
			}
			catch(Exception ex)
			{
				Functions.WriteLog($"Call [{storeName}] [Exception]:" + ex.ToString());
                this.CloseConnection(con);

				//if(this.debug)	throw ex;
				return null;
			}
		}
		public DataSet ExecuteStoredProcReturnDataSet(string sql,string[] paramNames,SqlDbType[] paramTypes,object[] paramValues)
		{
            return this.ExecuteStoredProcReturnDataSet(sql, paramNames, paramTypes, paramValues, false);
		}
		public DataSet ExecuteStoredProcReturnDataSet(string sql,string[] paramNames,SqlDbType[] paramTypes,object[] paramValues,out int returnValue)
		{
            return this.ExecuteStoredProcReturnDataSet(sql, paramNames, paramTypes, paramValues, out returnValue, false);
		}
		public DataSet ExecuteStoredProcReturnDataSet(string storeName,string[] paramNames,SqlDbType[] paramTypes,object[] paramValues,bool checkSchema)
		{
			int returnValue;
			return this.ExecuteStoredProcReturnDataSet(storeName,paramNames,paramTypes,paramValues,out returnValue,checkSchema);
		}
		public DataSet ExecuteStoredProcReturnDataSet(string storeName,string[] paramNames,SqlDbType[] paramTypes,object[] paramValues,out int returnValue,bool checkSchema)
		{
			returnValue = -1;
			if(paramNames == null || paramTypes == null || paramValues == null)	return null;
			int N = Math.Min(paramNames.Length,paramTypes.Length);
			N = Math.Min(N,paramValues.Length);

            SqlConnection con = this.connection;
			try
			{
				ArrayList schema = null;
				if(checkSchema)
				{
					schema = this.GetParamNamesArray(storeName);
					if(schema==null)
						throw new Exception("Cannot check schema !");
				}

				if(con==null)
				{
					con = new SqlConnection(this.connectionString);
					con.Open();
				}
				SqlCommand cmd = new SqlCommand(storeName,con);
				cmd.CommandType = CommandType.StoredProcedure;
				int count = 0;
				for(int i=0;i<N;i++)
				{
					if(checkSchema && !schema.Contains(Database.ProtectParamName(paramNames[i])))
						continue;
					SqlParameter param = new SqlParameter(Database.ProtectParamName(paramNames[i]),paramTypes[i]);
					param.Value = (paramValues[i]!=null) ? paramValues[i] : System.DBNull.Value;
					cmd.Parameters.Add(param);
					count++;
				}
				if(checkSchema && count<schema.Count)
				{
					string tmp = string.Empty;
					ArrayList tmpArray = new ArrayList(paramNames);
					foreach(string param in schema)
						if(!tmpArray.Contains(param))
							tmp += param + ",";
					throw new Exception("Database Alert: Not enough parameter(" + tmp.Trim(new char[]{','}) + ") to execute store proc !");
				}
				SqlParameter returnParam = cmd.Parameters.Add("@RETURN_VALUE",SqlDbType.Int);
				returnParam.Direction = ParameterDirection.ReturnValue;
				SqlDataAdapter adapter = new SqlDataAdapter(cmd);
				DataSet ds = new DataSet();
				adapter.Fill(ds);
				adapter.Dispose();
				cmd.Dispose();
				if(this.connection==null)
					con.Close();

                this.CloseConnection(con);

				returnValue = (int)returnParam.Value;
				return ds;
			}
			catch(Exception ex)
			{
                this.CloseConnection(con);

				if(this.debug)	throw ex;
				return null;
			}
		}
		#endregion Execute Store

		#region Auto
		public int ExecuteInsert(Hashtable data,string tableName)
		{
            return this.ExecuteInsert(data, tableName, false);
		}
		/// <summary>
		/// ExecuteInsert.
		/// </summary>
		/// <param name="data"></param>
		/// <param name="tableName"></param>
		public int ExecuteInsert(Hashtable data,string tableName,bool checkSchema)
		{
            if (data == null || data.Count == 0 || Functions.IsEmpty(tableName))
				return -1;

            SqlConnection con = this.connection;
			try
			{
				ArrayList schema = null;
				if(checkSchema)
				{
					schema = this.GetColumnNamesArray(tableName);
					if(schema==null)
						throw new Exception("Cannot check schema !");
				}
				SqlCommand cmd = new SqlCommand();
				object value = null;
				ArrayList fields = new ArrayList();
				foreach(string key in data.Keys)
				{
					if(checkSchema && !schema.Contains(Database.ProtectFieldName(key)))
						continue;
					fields.Add(key);
					value = data[key];
					value = value!=null ? value : System.DBNull.Value;
					cmd.Parameters.Add(new SqlParameter(Database.ProtectParamName(key),value));
				}

				cmd.CommandText = Database.CreateInsertSql(fields,tableName);
				
				if(con==null)
				{
					con = new SqlConnection(this.connectionString);
					con.Open();
				}
				cmd.Connection = con;
				int rs = cmd.ExecuteNonQuery();
				cmd.Dispose();
				if(this.connection==null)
					con.Close();

                this.CloseConnection(con);
				return rs;
			}
			catch(Exception ex)
			{
                this.CloseConnection(con);

				if(this.debug)	throw ex;
				return -1;
			}
		}
		public int ExecuteUpdate(Hashtable data,string tableName,string idField,object idValue)
		{
            return this.ExecuteUpdate(data, tableName, idField, idValue, false);
		}
		public int ExecuteUpdate(Hashtable data,string tableName,string idField,object idValue,bool checkSchema)
		{
            if (data == null || data.Count == 0 || Functions.IsEmpty(tableName))
				return -1;

            SqlConnection con = this.connection;
			try
			{
				ArrayList schema = null;
				if(checkSchema)
				{
					schema = this.GetColumnNamesArray(tableName);
					if(schema==null)
						throw new Exception("Cannot check schema !");
				}
				SqlCommand cmd = new SqlCommand();
				object value = null;
				ArrayList fields = new ArrayList();
				foreach(string key in data.Keys)
				{
					if(checkSchema && !schema.Contains(Database.ProtectFieldName(key)))
						continue;
					fields.Add(key);
					value = data[key];
					value = value!=null ? value : System.DBNull.Value;
					cmd.Parameters.Add(new SqlParameter(Database.ProtectParamName(key),value));
				}
				cmd.Parameters.Add(new SqlParameter(Database.ProtectParamName(idField),idValue));

				cmd.CommandText = Database.CreateUpdateSql(fields,tableName,idField);
				
				if(con==null)
				{
					con = new SqlConnection(this.connectionString);
					con.Open();
				}
				cmd.Connection = con;
				int rs = cmd.ExecuteNonQuery();
				cmd.Dispose();
				if(this.connection==null)
					con.Close();

                this.CloseConnection(con);

				return rs;
			}
			catch(Exception ex)
			{
                this.CloseConnection(con);

				if(this.debug)	throw ex;
				return -1;
			}
		}
		#endregion Auto

		#region Utility
		internal DataTable GetSchema(string tableName)
		{
			string sql = "SELECT * FROM " + tableName;
			return this.GetSchemaOfSql(sql);
		}
		internal DataTable GetSchemaOfSql(string sql)
		{
			SqlDataAdapter adapter = new SqlDataAdapter(sql,this.connectionString);
			adapter.MissingSchemaAction = MissingSchemaAction.AddWithKey;
			DataTable schema = new DataTable();
			adapter.FillSchema(schema,SchemaType.Source);
			adapter.Dispose();
			return schema;
		}
		internal DataTable GetColumns(string objectName)
		{
			string sql = "SELECT a.* FROM syscolumns a,sysobjects b WHERE b.name='" + objectName + "' and a.id=b.id";
			DataSet ds = this.ExecuteQuery(sql);
			return (ds!=null || ds.Tables.Count>0) ? ds.Tables[0] : null;
		}
		internal ArrayList GetColumnNamesArray(string objectName)
		{
			DataTable tb = this.GetColumnNames(objectName);
			if(tb==null)
				return null;
			ArrayList ar = new ArrayList(tb.Rows.Count);
			foreach(DataRow row in tb.Rows)
				ar.Add(row[0].ToString());
			return ar;
		}
		internal ArrayList GetParamNamesArray(string objectName)
		{
			DataTable tb = this.GetColumnNames(objectName);
			if(tb==null)
				return null;
			ArrayList ar = new ArrayList(tb.Rows.Count);
			string tmp = string.Empty;
			foreach(DataRow row in tb.Rows)
			{
				tmp = row[0].ToString();
                if (Functions.NotEmpty(tmp) && tmp[0] == '@')
					ar.Add(tmp);
			}				
			return ar;
		}
		internal DataTable GetColumnNames(string objectName)
		{
			string sql = "SELECT a.name FROM syscolumns a,sysobjects b WHERE b.name='" + objectName + "' and a.id=b.id";
			DataSet ds = this.ExecuteQuery(sql);
			return (ds!=null || ds.Tables.Count>0) ? ds.Tables[0] : null;
		}
		public static string CreateInsertSql(IEnumerable fields,string tableName)
		{
			StringBuilder s1 = new StringBuilder(500);
			s1.Append("INSERT INTO " + tableName + "(");
			StringBuilder s2 = new StringBuilder(500);
			s2.Append(" VALUES(");
			bool first = true;
			foreach(string field in fields)
			{
				if(first)
					first = false;
				else
				{
					s1.Append(",");
					s2.Append(",");
				}
				s1.Append(Database.ProtectFieldName(field));
				s2.Append(Database.ProtectParamName(field));
			}
			s1.Append(")");
			s2.Append(")");
			return s1.ToString() + s2.ToString();
		}
		public static string CreateUpdateSql(IEnumerable fields,string tableName,string idField)
		{
			StringBuilder s = new StringBuilder(500);
			s.Append("UPDATE " + tableName + " SET ");
			bool first = true;
			foreach(string field in fields)
			{
				if(first)
					first = false;
				else
					s.Append(",");
				s.Append(Database.ProtectFieldName(field) + "=" + Database.ProtectParamName(field));
			}
			s.Append(" WHERE " + Database.ProtectFieldName(idField) + "=" + Database.ProtectParamName(idField));
			return s.ToString();
		}
		private static string ProtectParamName(string paramName)
		{
			return paramName[0]=='@' ? paramName : ("@" + paramName);
		}
		private static string ProtectFieldName(string paramName)
		{
			return paramName[0]=='@' ? paramName.Substring(1) : paramName;
		}
		public static object ProtectNull(object value)
		{
			return value==null ? (object)System.DBNull.Value : value;
		}
		#endregion Utility
	}
}
