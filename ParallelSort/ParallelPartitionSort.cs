using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace BME.AUT.ParallelSort.CSharp
{
    /// <summary>
    /// Three phase parallel partition sorter
    /// </summary>
    public class ParallelPartitionSort : IParallelSort
    {
        private int shiftBits; // Number of bits to shift
        private int compBits;  // Number of bits to compare
        private int catCount;  // Number of categories
        private int cutOff;    // Maximal number
        private int min;       // Minimal number

        private int[] arr0, arr1; // Original array and sorted array

        // Step 1 - Count the members of each category (distributed)
        private int[] Step1(Int32 id)
        {
            // Get starting position for this task
            int start = id * (arr0.Length / catCount);
            // Get end position for this task
            int end = start + (arr0.Length / catCount);
            // Last task should process the remaining
            // (if the length is not divisible by the number of categories)
            if (id == catCount - 1) end = arr0.Length;
            // Count the members of each category
            int[] counts = new int[catCount];
            Array.Clear(counts, 0, counts.Length);
            for (int i = start; i < end; ++i)
            {
                // Members below the limit count in the 0th category
                if (arr0[i] < min) ++counts[0];
                // Members above the limit count in the last category
                else if ((arr0[i] - min) > cutOff) ++counts[catCount - 1];
                // Others are compared by the bits after shiftBits
                else ++counts[((arr0[i] - min) >> shiftBits) + 1];
            }
            // Return the result
            return counts;
        }

        // Helper class for step 2
        private class Step2Param
        {
            // Id of the task
            public int Id { get; set; }
            // Starting position for each category for this task
            public int[] Starts { get; set; }
        }

        // Step 2 - Copy each element into a new array to the position of its category (distributed)
        private void Step2(Step2Param param)
        {
            // Get task ID
            int id = param.Id;
            // Get starting position for this task
            int start = id * (arr0.Length / catCount);
            // Get end position for this task
            int end = start + (arr0.Length / catCount);
            // Last task should process the remaining
            // (if the length is not divisible by the number of categories)
            if (id == catCount - 1) end = arr0.Length;
            // Get starting position for each category
            int[] starts = param.Starts;
            // Process the elements
            int idx;
            for (int i = start; i < end; ++i)
            {
                // Get the category of the element
                if (arr0[i] < min) idx = 0;
                else if ((arr0[i]-min) > cutOff) idx = catCount - 1;
                else idx = ((arr0[i] - min) >> shiftBits) + 1;
                // Copy the element to its category in the new array
                arr1[starts[idx]] = arr0[i];
                // Increase the starting position of this category
                starts[idx]++;
            }
        }

        // Helper class for step 3
        private class Step3Param
        {
            // Starting position for this task
            public int Start { get; set; } 
            // Length of the segment for this task
            public int Length { get; set; }
        }

        // Step 3 - Sort the segments (distributed)
        private void Step3(Step3Param param)
        {
            Array.Sort(arr1, param.Start, param.Length);
        }

        /// <summary>
        /// Sort using the three step parallel partition sort
        /// </summary>
        /// <param name="array">Unsorted array</param>
        /// <returns>Sorted array</returns>
        public int[] Sort(int[] array)
        {
            // If the array is small, sort in one thread
            if (array.Length < 10000)
            {
                Array.Sort(array);
                return array;
            }
            // Store reference
            arr0 = array;
            // Calculate the number of bits for comparison
            compBits = Math.Min((int)Math.Log(Environment.ProcessorCount, 2) + 5 + (array.Length>10000000 ? 2 : 0), 12);
            // Get some random elements and estimate the minimum and maximum
            int max = 1;
            min = arr0[0];
            Random r = new Random();
            int samples = array.Length > 30000000 ? 100 : 1000;
            for (int i = 0; i < samples; i++)
            {
                int idx = r.Next() % arr0.Length;
                if (arr0[idx] > max) max = arr0[idx];
                else if (arr0[idx] < min) min = arr0[idx];
            }
            // Correction
            max -= min;
            // Count the number of bits to shift by knowing the maximum
            shiftBits = Math.Max((int)Math.Ceiling(Math.Log(max, 2)) - compBits, 0);
            // Calculate the maximal value rounded to a power of 2
            cutOff = 0;
            for (int i = 0; i < compBits + shiftBits; i++) cutOff = (cutOff << 1) + 1;
            // Number of categories
            catCount = (1 << compBits) + 2;
            
            // Step 1 - count elements
            Task<int[]>[] step1tasks = new Task<int[]>[catCount];
            for (int i = 0; i < catCount; i++)
                step1tasks[i] = Task.Factory.StartNew((x) => { return Step1((Int32)x); }, i);

            Task.WaitAll(step1tasks);
            
            // Accumulate step 1 results
            int[] totals = new int[catCount];
            Array.Clear(totals, 0, totals.Length);
            Parallel.For(0, catCount, (i) =>
            {
                for (int j = 0; j < catCount; j++)
                    totals[i] += step1tasks[j].Result[i];
            });
            
            // Starting positions for the first task
            List<int[]> starts = new List<int[]>(catCount);
            starts.Add(new int[catCount]);
            starts[0][0] = 0;
            for (int i = 1; i < catCount; i++) starts[0][i] = starts[0][i - 1] + totals[i - 1];
            // Starting positions for the other tasks
            for (int j = 1; j < catCount; j++)
            {
                int[] startsTmp = new int[catCount];
                for (int i = 0; i < catCount; i++) startsTmp[i] = starts[j - 1][i] + step1tasks[j - 1].Result[i];
                starts.Add(startsTmp);
            }

            // Step 2
            arr1 = new int[arr0.Length];
            Task[] step2tasks = new Task[catCount];
            for (int i = 0; i < catCount; i++)
            {
                step2tasks[i] = Task.Factory.StartNew((x) =>
                { Step2((Step2Param)x); }, new Step2Param() { Id = i, Starts = starts[i] });
            }

            Task.WaitAll(step2tasks);

            // Step 3
            Task[] step3tasks = new Task[catCount];
            int[] step3starts = new int[catCount];
            step3starts[0] = 0;
            for (int i = 1; i < catCount; i++) step3starts[i] = step3starts[i - 1] + totals[i - 1];
            //Console.Write("Sorting interval starts: "); PrintArray(step3starts);
            for (int i = 0; i < catCount; i++)
                step3tasks[i] = Task.Factory.StartNew((x) => { Step3((Step3Param)x); }, new Step3Param() { Start = step3starts[i], Length = totals[i] });

            Task.WaitAll(step3tasks);

            return arr1;
        }
    }
}
