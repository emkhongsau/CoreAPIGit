using CoreAPIGit.Common;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;
using System.Data;

namespace CoreAPIGit.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class WeatherForecastController : ControllerBase
    {
        public static IConfiguration _configuration;
        public WeatherForecastController(IConfiguration configuration)
        {
            _configuration = configuration;
        }

        [HttpGet(Name = "GetWeatherForecast")]
        public string Get()
        {
            var Connection = Functions.GetConfig_CS("MyConnectionString");
            //Database db = new Database(Connection);
            DataSet ds = Functions.db.ExecuteSPReturnDS("xp_sessionchat_getpage", new string[]{ "userisn", "botchatsisn" }, new object[] { 1004, 2001 });
            if(Functions.NotEmpty(ds))
            {

                return JsonConvert.SerializeObject(ds.Tables[0]);
            }
            return "";
        }
    }
}
