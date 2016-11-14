using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace BME.AUT.ParallelSort.CSharp
{
    public class SequentialSort : IParallelSort
    {
        public int[] Sort(int[] array)
        {
            Array.Sort(array);
            return array;
        }
    }
}
