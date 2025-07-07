using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowGui.App
{
    public class JobDefinition
    {
        public string Name { get; set; }
        public string Description { get; set; }
        public string Path { get; set; }

        public override string ToString()
        {
            return Name;
        }
    }

}
