using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CoreAPIGit.Attributes
{
    public class DBStoredParamsAttribute : Attribute
    {
        public string Name { get; set; }

        public DBStoredParamsAttribute()
        { }

        public DBStoredParamsAttribute(string name)
        {
            Name = name;
        }
    }
}
