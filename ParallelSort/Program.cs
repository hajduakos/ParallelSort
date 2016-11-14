using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;

namespace BME.AUT.ParallelSort.CSharp
{
    class Program
    {
        private static IParallelSort createSorter()
        {
            // instantiate your class
            return new ParallelPartitionSort();
            //return new SequentialSort();
        }

        static void Main(string[] args)
        {
            try
            {
                // ***********************
                // test correctness

                foreach (var size in new[] {0, 1, 10, 1000, 1000000})
                {
                    {
                        var input = getRandomIntArray(size, 453453);
                        var sorted = createSorter().Sort(input);
                        check(input, sorted);
                    }
                    {
                        var input = getSortedAscIntArray(size);
                        var sorted = createSorter().Sort(input);
                        check(input, sorted);
                    }
                    {
                        var input = getSortedDescIntArray(size);
                        var sorted = createSorter().Sort(input);
                        check(input, sorted);
                    }
                }


                // *********************
                // test performance

                using (var wr = new StreamWriter("parallelsort-performance.csv"))
                {
                    wr.WriteLine("array_size;execution time");
                    var timer = new Stopwatch();
                    foreach (var size in new[] {/*1000, 10000, 100000, 1000000, 2000000, 3000000, 4000000, 5000000, 10000000, 50000000,*/ 100000000})
                    {
                        var sorter = createSorter();
                        var input = getRandomIntArray(size, 456498);
                        timer.Restart();
                        // measured-start
                        sorter.Sort(input);
                        // measured-end
                        timer.Stop();
                        wr.WriteLine("{0};{1}", size, timer.ElapsedMilliseconds);
                        Console.WriteLine("{0};{1}", size, timer.ElapsedMilliseconds);
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
            Console.ReadKey();
        }

        private static void check(int[] reference, int[] sorted)
        {
            checkContent(reference, sorted);
            checkOrder(sorted);
        }

        private static void checkOrder(int[] array)
        {
            if (array.Length == 0)
                return;
            var previous = array[0];
            for (int i = 1; i < array.Length; ++i)
            {
                if (array[i] < previous)
                    throw new Exception(string.Format("Wrong order at index {0}.", i));
                previous = array[i];
            }
        }

        private static void checkContent(int[] reference, int[] sorted)
        {
            if (reference.Length != sorted.Length)
                throw new Exception("Sorted array does not contain the same elements.");

            var numsReference = new Dictionary<int, int>();
            foreach (var num in reference)
            {
                if (!numsReference.ContainsKey(num))
                    numsReference[num] = 1;
                else
                    ++numsReference[num];
            }

            var numsSorted = new Dictionary<int, int>();
            foreach (var num in sorted)
            {
                if (!numsSorted.ContainsKey(num))
                    numsSorted[num] = 1;
                else
                    ++numsSorted[num];
            }

            foreach (var x in numsReference)
            {
                if (!numsSorted.ContainsKey(x.Key) || numsSorted[x.Key] != x.Value)
                    throw new Exception("Sorted array does not contain the same elements.");
            }
        }

        private static int[] getRandomIntArray(int size, int seed)
        {
            var rnd = new Random(seed);
            return Enumerable.Range(0, size)
                             .Select(i => rnd.Next(0,int.MaxValue))
                             //.Select(i => Gauss(rnd.Next(0, int.MaxValue),int.MaxValue>>1))
                             .ToArray();
        }

        private static int[] getSortedAscIntArray(int size)
        {
            return Enumerable.Range(0, size).ToArray();
        }

        private static int[] getSortedDescIntArray(int size)
        {
            return Enumerable.Range(0, size).Reverse().ToArray();
        }

        static int Gauss(double x,double o)
        {
            return Math.Max(0, (int)(int.MaxValue*Math.Exp(-Math.Pow((x - o), 2))));
        }
    }
}
