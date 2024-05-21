using NLog;
using CoreAPIGit.Attributes;
using CoreAPIGit.Models;
using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Mail;
using System.Reflection;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;
using System.Web;
using System.Xml;
using static System.Collections.Specialized.BitVector32;

namespace CoreAPIGit.Common
{
    public class Functions
    {
        
        private static object locker = new object();
        private static NLog.Logger logger = LogManager.GetCurrentClassLogger();
        public static void WriteError(string format, params object[] args)
        {
            logger.Error(string.Format(format, args));
        }
        public static void WriteLogInfo(string format, params object[] args)
        {
            logger.Info(string.Format(format, args));
        }
        public static void WriteLogTrace(string format, params object[] args)
        {
            logger.Trace(string.Format(format, args));
        }
        

        #region Sql Encoder
        public static string EncodeSqlSpecialChars(string where)
        {
            if (where == null) return where;
            StringBuilder temp = new StringBuilder(where.Length + 10);
            foreach (char ch in where.ToCharArray())
            {
                if (ch == '\'')
                    temp.Append(new char[] { ch, ch });
                else if (ch == '%' || ch == '*' || ch == '[' || ch == ']')
                    temp.Append(new char[] { '[', ch, ']' });
                else
                    temp.Append(ch);
            }
            return temp.ToString();
        }
        public static string DecodeSqlSpecialChars(string where)
        {
            if (where == null) return where;
            StringBuilder temp = new StringBuilder(where.Length);
            char[] chs = where.ToCharArray();
            for (int i = 0; i < chs.Length; i++)
            {
                if (chs[i] == '\'')
                    temp.Append(chs[i++]);
                else if (chs[i] == '[')
                {
                    temp.Append(chs[i + 1]);
                    i += 2;
                }
                else
                    temp.Append(chs[i]);
            }
            return temp.ToString();
        }
        #endregion Sql Encoder

        public static string HttpMethodGet(string strUrl)
        {
            try
            {
                System.Net.ServicePointManager.SecurityProtocol = System.Net.SecurityProtocolType.Tls12;
                if (!string.IsNullOrEmpty(strUrl))
                {
                    WriteLogInfo("[HttpMethodGet] Url: {0}", strUrl);

                    var request = (HttpWebRequest)WebRequest.Create(strUrl);
                    request.Timeout = 30000;
                    HttpWebResponse response;
                    try
                    {
                        response = (HttpWebResponse)request.GetResponse();
                    }
                    catch (WebException ex)
                    {
                        response = (HttpWebResponse)ex.Response;
                    }



                    var dataStream = new StreamReader(response.GetResponseStream());
                    //show the response string on the console screen.
                    string xData = dataStream.ReadToEnd();
                    dataStream.Close();

                    WriteLogInfo("[HttpMethodGet] Return data Length: {0}", xData.Length);
                    return xData;
                }
                return "";
            }
            catch (Exception ex)
            {
                WriteError("[HttpMethodGet] Error. Url=" + strUrl, ex);
                return "";
            }
        }
        public static string ToMD5Hash(string input)
        {
            MD5 md5 = System.Security.Cryptography.MD5.Create();
            byte[] inputBytes = System.Text.Encoding.ASCII.GetBytes(input);
            byte[] hash = md5.ComputeHash(inputBytes);

            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < hash.Length; i++)
            {
                sb.Append(hash[i].ToString("X2"));
            }
            return sb.ToString();
        }
        public static void WriteLog(string format)
        {
            lock (locker)
            {
                try
                {
                    string logFileName = string.Format(GetConfig_CS("LogFile"), DateTime.Today.ToString("MMMdd.yyyy"));
                    FileInfo file = new FileInfo(logFileName);
                    if (!file.Directory.Exists)
                        file.Directory.Create();

                    if (!file.Exists)
                        file.CreateText().Close();
                    using (StreamWriter ws = file.AppendText())
                    {
                        ws.WriteLine(DateTime.Now.ToString("hh:mm:ss.fff tt") + " - " + format);
                    }
                }
                catch
                {
                }
            }
        }

        public static string ToBase64(string text)
        {
            return ToBase64(text, Encoding.UTF8);
        }

        public static string ToBase64(string text, Encoding encoding)
        {
            if (string.IsNullOrEmpty(text))
            {
                return text;
            }

            byte[] textAsBytes = encoding.GetBytes(text);
            return Convert.ToBase64String(textAsBytes);
        }

        public static bool TryParseBase64(string text, out string decodedText)
        {
            return TryParseBase64(text, Encoding.UTF8, out decodedText);
        }

        public static bool TryParseBase64(string text, Encoding encoding, out string decodedText)
        {
            if (string.IsNullOrEmpty(text))
            {
                decodedText = text;
                return false;
            }

            try
            {
                byte[] textAsBytes = Convert.FromBase64String(text);
                decodedText = encoding.GetString(textAsBytes);
                return true;
            }
            catch (Exception)
            {
                decodedText = null;
                return false;
            }
        }
       
        public static IConfiguration _configuration =  new ConfigurationBuilder()
            .AddJsonFile("appsettings.json")
            .Build();

        //public Functions(IConfiguration configuration)
        //{
        //    _configuration = configuration;
        //}
        private static Database database = null;
        public static Database db
        {
            get
            {
                if (database == null)
                {
                    database = new Database(ConnectionString);
                    //Globals.database.Debug = true;
                }
                return database;
            }
        }
        public static string ConnectionString
        {
            get
            {
                return _configuration.GetConnectionString("MyConnectionString");
            }
        }


        public static string GetConfig_CS(string name)
        {
            try
            {
                return _configuration.GetConnectionString(name);
            }
            catch (Exception) { }
            return "";
        }

        public static DateTime? ConvertToDate(object date)
        {
            try
            {
                if (date == null) return null;
                return Convert.ToDateTime(date);
            }
            catch (Exception ex)
            {
            }
            return null;
        }
        public static DateTime? AddTimezoneDate(DateTime date, double TotalHours)
        {
            try
            {
                if (date == null) return null;
                return date.AddHours(TotalHours);
            }
            catch (Exception ex)
            {
            }
            return null;
        }
        public static string ToString(object s)
        {
            try
            {
                if (s != null) return s.ToString();
                return "";
            }
            catch (Exception ex)
            {
            }
            return "";
        }
       

        public static string HttpMethodPostXMLData(string url, string dataXML)
        {
            try
            {
                System.Net.ServicePointManager.SecurityProtocol = System.Net.SecurityProtocolType.Tls12;
                var request = (HttpWebRequest)WebRequest.Create(url);
                byte[] bytes;
                bytes = System.Text.Encoding.ASCII.GetBytes(dataXML);
                request.ContentType = "text/xml; encoding='utf-8'";
                request.ContentLength = bytes.Length;
                request.Method = "POST";
                Stream requestStream = request.GetRequestStream();
                requestStream.Write(bytes, 0, bytes.Length);
                requestStream.Close();
                var response = (HttpWebResponse)request.GetResponse();
                if (response.StatusCode == HttpStatusCode.OK)
                {
                    Stream responseStream = response.GetResponseStream();
                    string responseStr = new StreamReader(responseStream).ReadToEnd();
                    return responseStr;
                }
            }
            catch (Exception ex)
            {

            }
            return string.Empty;
        }
        public static DataSet ConvertXMLToDataSet(string xmlData)
        {
            StringReader stream = null;
            XmlTextReader reader = null;
            try
            {
                DataSet xmlDS = new DataSet();
                stream = new StringReader(xmlData);
                // Load the XmlTextReader from the stream
                reader = new XmlTextReader(stream);
                xmlDS.ReadXml(reader);
                return xmlDS;
            }
            catch
            {
                return null;
            }
            finally
            {
                if (reader != null) reader.Close();
            }
        }
        
        public static void SendMail(string to, string subject, string body)
        {
            var from = GetConfig_CS("UserNameMail");
            var fromName = GetConfig_CS("FullNameMail");
            var pass = GetConfig_CS("PasswordMail");
            var msg = new MailMessage();
            msg.From = new MailAddress(from, fromName);
            msg.To.Add(new MailAddress(to));
            msg.Subject = subject;
            msg.Body = body;
            msg.IsBodyHtml = true;
            try
            {
                var smtpServer = GetConfig_CS("SMTP_Server");
                var smtpPort = GetConfig_CS("SMTP_Port");
                var boolEnableSsl = Convert.ToBoolean(GetConfig_CS("EnableSsl"));
                var boolDefaultCredentials = Convert.ToBoolean(GetConfig_CS("DefaultCredentials"));
                var client = new SmtpClient(smtpServer, ConvertObjectToInt(smtpPort, 0));
                client.EnableSsl = boolEnableSsl;

                if (!boolDefaultCredentials)
                {
                    var cre = new NetworkCredential(from, pass);
                    client.UseDefaultCredentials = boolDefaultCredentials;
                    client.Credentials = cre;
                }
                else
                {
                    client.UseDefaultCredentials = boolDefaultCredentials;
                }
                client.Send(msg);
                WriteLog(string.Format("Send email: Successful"));
            }
            catch (Exception ex)
            {
                WriteLog(string.Format("Send email: UserName: {0} - Password: {1} - Message: {2}", from, pass, ex.Message));
            }
        }

        public static void SendMail(string to, string subject, string body, List<string> pathFiles)
        {
            var from = GetConfig_CS("UserNameMail");
            var fromName = GetConfig_CS("FullNameMail");
            var pass = GetConfig_CS("PasswordMail");
            var msg = new MailMessage();
            msg.BodyEncoding = Encoding.UTF8;
            msg.From = new MailAddress(from, fromName);
            msg.To.Add(new MailAddress(to));
            msg.Subject = subject;
            msg.Body = body;
            msg.IsBodyHtml = true;
            foreach (var item in pathFiles)
            {
                WriteLogInfo("Send email: Path Files Attachment: {0}", item);
                Attachment at = new Attachment(item);
                msg.Attachments.Add(at);
            }
            try
            {
                var smtpServer = GetConfig_CS("SMTP_Server");
                var smtpPort = GetConfig_CS("SMTP_Port");
                var boolEnableSsl = Convert.ToBoolean(GetConfig_CS("EnableSsl"));
                var boolDefaultCredentials = Convert.ToBoolean(GetConfig_CS("DefaultCredentials"));
                var client = new SmtpClient(smtpServer, ConvertObjectToInt(smtpPort, 0));
                client.EnableSsl = boolEnableSsl;

                if (!boolDefaultCredentials)
                {
                    var cre = new NetworkCredential(from, pass);
                    client.UseDefaultCredentials = boolDefaultCredentials;
                    client.Credentials = cre;
                }
                else
                {
                    client.UseDefaultCredentials = boolDefaultCredentials;
                }
                client.Send(msg);
                WriteLog("Send email: Successful");
            }
            catch (Exception ex)
            {
                WriteLog(string.Format("Send email: UserName: {0} - Password: {1} - Message: {2}", from, pass, ex.Message));
            }
        }
        public static bool WriteFileHtml(string dataHtml, string pathSave)
        {
            try
            {
                using (FileStream fs = new FileStream(pathSave, FileMode.Create))
                {
                    using (StreamWriter w = new StreamWriter(fs, Encoding.UTF8))
                    {
                        w.WriteLine(dataHtml);
                    }
                }
                return true;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            return false;
        }



        public static void WriteFileFromBase64(string fname, string sBase64Data)
        {
            try
            {
                string sPath = Path.GetDirectoryName(fname);
                if (!Directory.Exists(sPath)) Directory.CreateDirectory(sPath);

                File.WriteAllBytes(fname, Convert.FromBase64String(sBase64Data));
            }
            catch (Exception e)
            {
                try
                {
                    string dummyData = sBase64Data.Trim().Replace(" ", "+");
                    if (dummyData.Length % 4 > 0)
                        dummyData = dummyData.PadRight(dummyData.Length + 4 - dummyData.Length % 4, '=');

                    File.WriteAllBytes(fname, Convert.FromBase64String(dummyData));
                }
                catch (Exception ex)
                {
                    WriteLog("[Function.WriteFileFromBase64] Exception: " + ex.ToString());
                }
            }
        }

        public static string ConvertFileToBase64(string sFullFilePath)
        {
            try
            {
                Byte[] bytesData = File.ReadAllBytes(sFullFilePath);
                return Convert.ToBase64String(bytesData);
            }
            catch (Exception ex)
            {
                WriteLog("[Function.ConvertFileToBase64] Exception: " + ex.ToString());
                return "";
            }
        }

        public static string Report3B_RemoveTradeLine(string sFullContent)
        {
            //string sFullContent = Functions.readFromFile(sFilePath, true);

            //string sNewFile = Path.Combine(Path.GetDirectoryName(sFilePath), Path.GetFileNameWithoutExtension(sFilePath) + "_newfile.html");

            sFullContent = Regex.Replace(sFullContent, "xmlns:d1p1=\"(.*?)\"", "");
            sFullContent = Regex.Replace(sFullContent, "<img (.*?)[^\\>]+>", "");

            sFullContent = Regex.Replace(sFullContent, "	", " ");
            sFullContent = Regex.Replace(sFullContent, " {2,}", " ");
            sFullContent = Regex.Replace(sFullContent, " >", ">");
            sFullContent = Regex.Replace(sFullContent, "\n", "");
            sFullContent = Regex.Replace(sFullContent, "<br>", "<br>\n");
            sFullContent = Regex.Replace(sFullContent, "</tr>", "</tr>\n");
            sFullContent = sFullContent.Replace("®", "&reg;");


            //Functions.WriteFileHtml(sFullContent, sNewFile);
            return sFullContent;
        }

        public static StringBuilder ReadHtmlFile(string htmlFileNameWithPath)
        {
            System.Text.StringBuilder htmlContent = new System.Text.StringBuilder();
            string line;
            try
            {
                using (System.IO.StreamReader htmlReader = new System.IO.StreamReader(htmlFileNameWithPath))
                {
                    while ((line = htmlReader.ReadLine()) != null)
                    {
                        htmlContent.Append(line);
                    }
                }
            }
            catch (Exception objError)
            {
                throw objError;
            }

            return htmlContent;
        }
        public static string getValueByID(string id, DataTable tb)
        {
            try
            {
                string s = "";
                DataRow[] arrRow2 = tb.Select("attID = '" + id + "'");
                if (arrRow2.Length > 0)
                {
                    s = arrRow2[0]["attValue"].ToString();
                }
                return s;
            }
            catch (Exception ex)
            {
                return string.Empty;
            }
        }
        public static string HttpPost_Json(string sURL, string jsonData)
        {
            WriteLog("[Function.HttpPost] ---- Begin ---- ");

            string rs = "";
            WriteLog("[Function.HttpPost] sURL: " + sURL);
            WriteLog("[Function.HttpPost] jsonData: " + jsonData);
            System.Net.ServicePointManager.SecurityProtocol = System.Net.SecurityProtocolType.Tls12;


            try
            {
                HttpWebRequest req = WebRequest.Create(sURL) as HttpWebRequest;
                req.ContentType = "application/json";
                req.Method = "POST";

                byte[] byteArray = Encoding.UTF8.GetBytes(jsonData);
                req.ContentLength = byteArray.Length;


                WriteLog("[Function.HttpPost] Send Request...");
                Stream postStream = req.GetRequestStream();
                postStream.Write(byteArray, 0, byteArray.Length);
                postStream.Flush();
                postStream.Close();


                using (WebResponse resp = req.GetResponse())
                {
                    using (StreamReader reader = new StreamReader(resp.GetResponseStream()))
                    {
                        rs = reader.ReadToEnd();
                    }
                }
            }
            catch (Exception ex)
            {
                WriteLog("[Function.HttpPost] Exception: " + ex.ToString());
            }

            WriteLog("[Function.HttpPost] Return value: " + rs);
            WriteLog("[Function.HttpPost] ---- End ---- ");

            return rs;
        }
        public static string RemoveLeadingNumeric(string input)
        {
            int num = 0;
            foreach (char c in input)
            {
                if (!char.IsDigit(c) && c != '_')
                {
                    break;
                }

                num++;
            }

            return input.Substring(num);
        }
        public static string RemoveSpecialChar_Sqlite(string sName)
        {
            string input = Regex.Replace(sName, "[^a-zA-Z0-9_]", "").Trim();
            input = RemoveLeadingNumeric(input).Trim();
            if (input == "")
            {
                input = "NoName";
            }

            return input;
        }
        
        public static double ConvertToDouble(object obj)
        {
            return ConvertToDouble(obj, 0.0);
        }

        public static double ConvertToDouble(object s, double defaultvalue)
        {
            try
            {
                return Convert.ToDouble(unformatNumber(s.ToString()));
            }
            catch
            {
                return defaultvalue;
            }
        }
        public static bool AreAllColumnsEmpty(DataRow dr)
        {
            if (dr == null)
            {
                return true;
            }
            else
            {
                foreach (var value in dr.ItemArray)
                {
                    if (value != null)
                    {
                        return false;
                    }
                }
                return true;
            }
        }
       
        public static string HttpPost(string sWS_URL, string postData, out string sErr)
        {
            WriteLog("[Functions.HttpPost] ---- Begin ---- ");
            sErr = "";
            ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;

            string rs = "";
            WriteLog("[Functions.HttpPost] sURL: " + sWS_URL);
            WriteLog("[Functions.HttpPost] postData: " + postData);

            try
            {
                HttpWebRequest req = WebRequest.Create(sWS_URL) as HttpWebRequest;
                req.ContentType = "application/x-www-form-urlencoded";
                req.Method = "POST";
                //req.KeepAlive = false;


                byte[] byteArray = System.Text.Encoding.UTF8.GetBytes(postData);

                req.ContentLength = byteArray.Length;


                Stream postStream = req.GetRequestStream();
                postStream.Write(byteArray, 0, byteArray.Length);
                postStream.Flush();
                postStream.Close();


                //Check Authorized
                HttpWebResponse resp;
                using (resp = (HttpWebResponse)req.GetResponse())
                {
                    using (StreamReader reader = new StreamReader(resp.GetResponseStream()))
                    {
                        rs = reader.ReadToEnd();
                    }
                }
            }
            catch (WebException we)
            {
                try
                {

                    HttpWebResponse response = (HttpWebResponse)we.Response;
                    //var obj = response.GetResponseStream();
                    using (StreamReader reader = new StreamReader(response.GetResponseStream()))
                    {
                        sErr = reader.ReadToEnd();
                    }
                }
                catch (Exception ex)
                {
                    WriteLog("[Functions.HttpPost] Exception: " + ex.ToString());
                    sErr = ex.Message;
                }
            }

            WriteLog("[Functions.HttpPost] Return value: " + rs);
            WriteLog("[Functions.HttpPost] ---- End ---- ");

            return rs;
        }

        public static string HttpGet(string sWS_URL, out string sErr)
        {
            WriteLog("[Functions.HttpGet] ---- Begin ---- ");
            sErr = "";

            ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;

            string rs = "";
            WriteLog("[Functions.HttpGet] sURL: " + sWS_URL);

            try
            {
                HttpWebRequest req = WebRequest.Create(sWS_URL) as HttpWebRequest;
                req.Method = "GET";
                

                WebResponse resp;
                using (resp = req.GetResponse())
                {
                    using (StreamReader reader = new StreamReader(resp.GetResponseStream()))
                    {
                        rs = reader.ReadToEnd();
                    }
                }
            }
            catch (WebException we)
            {
                try
                {

                    HttpWebResponse response = (HttpWebResponse)we.Response;
                    //var obj = response.GetResponseStream();
                    using (StreamReader reader = new StreamReader(response.GetResponseStream()))
                    {
                        sErr = reader.ReadToEnd();
                    }
                }
                catch (Exception ex)
                {
                    sErr = ex.Message;
                }
            }

            //WriteLog("[Functions.HttpGet] Return value: " + rs);
            WriteLog("[Functions.HttpGet] ---- End ---- ");

            return rs;
        }
        public static bool CheckVersionForm(DateTime? createdataOrg, DateTime? createdataSubmit, long formisn)
        {
            bool resp = false;
            if(createdataOrg.HasValue && createdataOrg.Value >= DateTime.Parse("07/25/2022 00:00:00") || formisn == 2009112)
            {
                return true;
            }
            if(createdataSubmit.HasValue && createdataSubmit.Value >= DateTime.Parse("07/25/2022 00:00:00") && formisn== 2009112)
            {
                return true;
            }
            return resp;
        }

        public static bool IsEmpty(DataSet ds)
        {
            return ds == null || ds.Tables.Count == 0 || ds.Tables[0].Rows.Count == 0;
        }
        public static bool IsEmpty(DataTable table)
        {
            return table == null || table.Rows.Count == 0;
        }
        public static bool IsEmpty(string str)
        {
            return !NotEmpty(str);
        }
        public static bool NotEmpty(DataSet ds)
        {
            return ds != null && ds.Tables.Count > 0 && ds.Tables[0].Rows.Count > 0;
        }
        public static bool NotEmpty_Mutiltable(DataSet ds)
        {
            return ds != null && ds.Tables.Count > 0;
        }
        public static bool NotEmpty(DataTable table)
        {
            return table != null && table.Rows.Count > 0;
        }
        public static bool NotEmpty(Guid g)
        {
            return g != null && g!=Guid.Empty;
        }
        public static bool NotEmpty(string str)
        {
            if (str != null && str != string.Empty)
                return true;
            return false;
        }

        public static int ConvertObjectToInt(object obj, int defaultValue)
        {
            try
            {
                if (obj.ToString().IndexOf(".") > -1)
                    obj = obj.ToString().Substring(0, obj.ToString().IndexOf("."));

                return Convert.ToInt32(obj);
            }
            catch (Exception ex) { string a = ex.Message; }
            return defaultValue;
        }
        public static int ConvertObjectToInt(object obj)
        {
            return ConvertObjectToInt(obj, 0);
        }
        public static decimal ConvertObjectToDecimal(object obj, decimal defaultValue)
        {
            try
            {
                if (obj == null || obj is DBNull || obj.ToString() == "")
                    return defaultValue;
                return Convert.ToDecimal(obj.ToString().Trim().Replace(",", "")
                                                                .Replace("$", ""));
            }
            catch (Exception)
            { }
            return defaultValue;
        }
        public static decimal ConvertObjectToDecimal(object obj)
        {
            return ConvertObjectToDecimal(obj, 0);
        }
        public static string formatPhone(string phone, char Separator, int glength)
        {
            if (phone.Length < glength * 2)
                return phone;
            else
                return phone.Substring(0, glength) + Separator.ToString() + formatPhone(phone.Substring(glength, phone.Length - glength), Separator, glength);
        }
        public static string unformatPhone(string phoneNum)
        {
            if (phoneNum == null)
                return "";

            string s = phoneNum.Replace("(", "");
            s = s.Replace(")", "")
                .Replace("-", "")
                .Replace(" ", "");
            return s;
        }
        public static string GetMaskedText(string plainText, string mask)
        {
            if (IsEmpty(plainText) || IsEmpty(mask))
                return plainText;

            StringBuilder text = new StringBuilder();
            char ch = ' ';
            int index = 0;
            int pLength = plainText.Length;
            for (int i = 0; (i < mask.Length && index < pLength); i++, index++)
            {
                switch (mask[i])
                {
                    case '#':	//	Number
                        while (index < pLength && (ch = plainText[index]) == ch && (ch < '0' || ch > '9')) index++;
                        if (index < pLength) text.Append(ch);
                        break;
                    case '*':	//	Letter or Number
                        while (index < pLength && (ch = plainText[index]) == ch && (ch < '0' || (ch > '9' && ch < 'A') || (ch > 'Z' && ch < 'a') || ch > 'z')) index++;
                        if (index < pLength) text.Append(ch);
                        break;
                    case '$':	//	Letter
                        while (index < pLength && (ch = plainText[index]) == ch && (ch < 'A' || (ch > 'Z' && ch < 'a') || ch > 'z')) index++;
                        if (index < pLength) text.Append(ch);
                        break;
                    default:	//	Match exactly.
                        if (plainText[index] != mask[i]) index--;
                        text.Append(mask[i]);
                        break;
                    case '+':	//	Match previous mask any times.
                        if (i > 0)
                        {
                            switch (mask[i - 1])
                            {
                                case '#':	//	Number
                                    while (index < pLength && (ch = plainText[index]) == ch && (ch >= '0' && ch <= '9'))
                                    {
                                        index++;
                                        text.Append(ch);
                                    }
                                    break;
                                case '*':	//	Letter or Number
                                    while (index < pLength && (ch = plainText[index]) == ch && ((ch >= '0' && ch <= '9') || (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z')))
                                    {
                                        index++;
                                        text.Append(ch);
                                    }
                                    break;
                                case '$':	//	Letter
                                    while (index < pLength && (ch = plainText[index]) == ch && ((ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch > 'z')))
                                    {
                                        index++;
                                        text.Append(ch);
                                    }
                                    break;
                                default:	//	Match exactly.
                                    while (index < pLength && (ch = plainText[index]) == ch && (ch == mask[i - 1]))
                                    {
                                        index++;
                                        text.Append(ch);
                                    }
                                    break;
                            }
                        }
                        index--;
                        break;
                }
            }
            return text.ToString();
        }
        public static String readFromFile(String pathFullFile, bool isVietNam)
        {
            try
            {
                StreamReader reader = new StreamReader(pathFullFile, isVietNam ? System.Text.Encoding.UTF8 : System.Text.Encoding.ASCII);
                String strTemp = reader.ReadToEnd();
                reader.Close();
                return strTemp;
            }
            catch (Exception) { }
            return "";
        }

        public static string SubString(string sContent, int iStart, int iEnd)
        {
            try
            {
                return sContent.Substring(iStart, iEnd - iStart);
            }
            catch (Exception) { }

            return "";
        }



        public static string RandomText_NoIL(int type, int length)
        {
            string[] strArray = new string[]{"0","1","2","3","4","5","6","7","8","9",
 											 "a","b","c","d","e","f","g","h","j",
											 "k","m","n","o","p","q","r","s","t",
											 "u","v","w","x","y","z",
											 "A","B","C","D","E","F","G","H","J",
											 "K","M","N","O","P","Q","R","S","T",
											 "U","V","W","X","Y","Z",};
            string ret = "";
            Random rd = new Random();
            if (length == 0) length = rd.Next(50);
            switch (type)
            {
                case 0: // number
                    for (int i = 0; i < length; i++)
                        ret += strArray[rd.Next(0, 9)];
                    break;
                case 1: // character
                    for (int i = 0; i < length; i++)
                        ret += strArray[rd.Next(10, 57)];
                    break;
                default: // number and character
                    for (int i = 0; i < length; i++)
                        ret += strArray[rd.Next(0, 57)];
                    break;
            }
            return ret;
        }
        public static string RemoveSpecialLetter(string s)
        {
            string stFormD = s.Normalize(NormalizationForm.FormD);
            StringBuilder sb = new StringBuilder();
            for (int ich = 0; ich < stFormD.Length; ich++)
            {
                System.Globalization.UnicodeCategory uc = System.Globalization.CharUnicodeInfo.GetUnicodeCategory(stFormD[ich]);
                if (uc == UnicodeCategory.LowercaseLetter || uc == UnicodeCategory.UppercaseLetter
                    || uc == UnicodeCategory.LineSeparator || uc == UnicodeCategory.SpaceSeparator
                     || uc == UnicodeCategory.DecimalDigitNumber)
                {
                    sb.Append(stFormD[ich]);
                }
            }
            sb = sb.Replace('Đ', 'D');
            sb = sb.Replace('đ', 'd');
            return (sb.ToString().Normalize(NormalizationForm.FormD));
        }
        public static string FormatPriceToString(object value)
        {
            var valueDecimal = ConvertObjectToDecimal(value);
            string price = (valueDecimal != 0) ? valueDecimal.ToString("$#,#0.00;-$#,#0.00") : "$0.00";
            return price;
        }
        public static string formatSSN(string SSN, char Separator)
        {
            SSN = unformatPhone(SSN);
            return GetMaskedText(SSN, "###-##-####");
        }

        public static string formatZip(string zip)
        {
            zip = unformatPhone(zip);
            return GetMaskedText(zip, "#####-####");
        }
        public static string unformatNumber(string Number)
        {
            string s = Number.Replace(",", "").Replace(" ", "");
            if (s.IndexOf(".") > -1)
            {
                if (s.Split('.').Length > 2)
                {
                    // 123.00.12.32
                    int idx = s.IndexOf('.', 0);
                    idx = s.IndexOf('.', (idx + 1));

                    s = s.Substring(0, idx);
                }
            }

            return s;
        }
        public static string Report3B_Hilight(string sFullContent, string sAttr) {
            string sContent = "";

            while (sFullContent.IndexOf(sAttr) > -1)
            {
                #region
                string sBegin = sFullContent.Substring(0, sFullContent.IndexOf(sAttr));
                string sEnd = sFullContent.Substring(sFullContent.IndexOf(sAttr) + sAttr.Length);

                string sAttrClass = "tr class=\"";
                string s1 = sBegin.Substring(0, sBegin.LastIndexOf(sAttrClass)) + sAttrClass + "hightlight " + sBegin.Substring(sBegin.LastIndexOf(sAttrClass) + sAttrClass.Length);
                
                //
                sContent += s1 + sAttr;

                sFullContent = sEnd;
                #endregion
            }
            sContent += sFullContent;

            return sContent;
        }



        // get parameters
        public static Hashtable GetParameters(object obj)
        {
            Type type = obj.GetType();
            List<PropertyInfo> listProperty = new List<PropertyInfo>(type.GetProperties());
            Hashtable result = new Hashtable();
            foreach (PropertyInfo prop in listProperty)
            {
                result.Add(prop.Name, prop.GetValue(obj));
            }

            return result;
        }

        public static ResultDTO<List<T>> DataTableToList<T>(DataTable table) where T : new()
        {
            try
            {
                if (table == null || table.Rows.Count == 0)
                {
                    return new ResultDTO<List<T>>()
                    {
                        Code = -101,
                        Message = "invalid data"
                    };
                }

                List<T> result = new List<T>();
                foreach (DataRow row in table.Rows)
                {
                    var item = CreateObjectWithDataRow<T>(row);
                    result.Add(item);
                }

                return new ResultDTO<List<T>>()
                {
                    Code = 0,
                    Data = result
                };
            }
            catch (Exception ex)
            {
                WriteError("Exception DataTableToList", ex);
                return new ResultDTO<List<T>>()
                {
                    Code = ex.HResult,
                    Message = ex.Message
                };
            }
        }
        public static ResultDTO<List<T>> DataRowsToList<T>(List<DataRow> rows) where T : new()
        {
            try
            {
                if (rows == null || rows.Count == 0)
                {
                    return new ResultDTO<List<T>>()
                    {
                        Code = -101,
                        Message = "invalid data"
                    };
                }

                List<T> result = new List<T>();
                foreach (DataRow row in rows)
                {
                    var item = CreateObjectWithDataRow<T>(row);
                    result.Add(item);
                }

                return new ResultDTO<List<T>>()
                {
                    Code = 0,
                    Data = result
                };
            }
            catch (Exception ex)
            {
                WriteError("Exception DataTableToList", ex);
                return new ResultDTO<List<T>>()
                {
                    Code = ex.HResult,
                    Message = ex.Message
                };
            }
        }

        public static T CreateObjectWithDataRow<T>(DataRow row) where T : new()
        {
            try
            {
                Type type = typeof(T);
                var result = new T();

                foreach (PropertyInfo prop in type.GetProperties())
                {
                    var attributes = prop.GetCustomAttributes(false);
                    string colName = prop.Name;

                    try
                    {
                        var colMapping = attributes.FirstOrDefault(a => a.GetType() == typeof(ColumnAttribute));
                        if (colMapping != null)
                        {
                            colName = (colMapping as DbColumnAttribute).Name;
                        }

                        if (!row.Table.Columns.Contains(colName))
                        {
                            continue;
                        }

                        if (row[colName] == DBNull.Value)
                        {
                            prop.SetValue(result, null);
                            continue;
                        }

                    
                        if(prop.PropertyType == typeof(byte))
                        {
                            prop.SetValue(result, int.Parse(row[colName].ToString()));
                        }
                        else
                        {
                            prop.SetValue(result, row[colName]);
                        }
                        
                    }
                    catch(Exception ex)
                    {
                        //WriteLog("[Exception] CreateObjectWithDataRow colName:{0} - Ex:", colName, ex);
                        //prop.SetValue(result, Convert.ChangeType(row[colName], prop.PropertyType));
                    }
                }

                return result;
            }
            catch (Exception ex)
            {
                WriteError("CreateObjectWithDataRow Exception:", ex);
                return new T();
            }
        }

  

        public static DataTable ToDataTable<T>(IList<T> data)
        {
            DataTable table = new DataTable();

            //special handling for value types and string
            if (typeof(T).IsValueType || typeof(T).Equals(typeof(string)))
            {

                DataColumn dc = new DataColumn("Value", typeof(T));
                table.Columns.Add(dc);
                foreach (T item in data)
                {
                    DataRow dr = table.NewRow();
                    dr[0] = item;
                    table.Rows.Add(dr);
                }
            }
            else
            {
                PropertyDescriptorCollection properties = TypeDescriptor.GetProperties(typeof(T));
                foreach (PropertyDescriptor prop in properties)
                {
                    table.Columns.Add(prop.Name, Nullable.GetUnderlyingType(prop.PropertyType) ?? prop.PropertyType);
                }
                foreach (T item in data)
                {
                    DataRow row = table.NewRow();
                    foreach (PropertyDescriptor prop in properties)
                    {
                        try
                        {
                            row[prop.Name] = prop.GetValue(item) ?? DBNull.Value;
                        }
                        catch (Exception ex)
                        {
                            row[prop.Name] = DBNull.Value;
                        }
                    }
                    table.Rows.Add(row);
                }
            }
            return table;
        }

        public static string getRequired(int type)
        {
            var style = string.Empty;
            switch (type)
            {
                case 1:
                    style = "required";
                    break;
            }
            return style;
        }
        public static bool IsSameDay(DateTime datetime1, DateTime datetime2)
        {
            return datetime1.Year == datetime2.Year
                && datetime1.Month == datetime2.Month
                && datetime1.Day == datetime2.Day;
        }
        public static string RandomString(int size)
        {
            StringBuilder builder = new StringBuilder();
            Random random = new Random();
            char ch;
            for (int i = 0; i < size; i++)
            {
                ch = Convert.ToChar(Convert.ToInt32(Math.Floor(26 * random.NextDouble() + 65)));
                builder.Append(ch);

            }
            return builder.ToString();
        }
       
    }
}