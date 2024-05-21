using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace CoreAPIGit.Attributes
{
    public class DbColumnAttribute : Attribute
    {
        public string Name { get; set; }
        public DbColumnAttribute()
        { }

        public DbColumnAttribute(string name)
        {
            Name = name;
        }
    }
}