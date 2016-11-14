using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

namespace BME.AUT.ParallelSort.CSharp
{
    public class ParallelMergeSort : IParallelSort
    {
        private int[] blockStarts;
        private int[] blockSizes;
        private int[] arr;

        public int[] Sort(int[] array)
        {
            if (array.Length < Math.Max(Environment.ProcessorCount, 1000))
            {
                Array.Sort(array);
                return array;
            }

            int nBlocks = Environment.ProcessorCount;

            blockStarts = new int[nBlocks];
            blockSizes = new int[nBlocks];
            int defBlockSize = array.Length / nBlocks;

            int i, j;

            arr = array;

            for (i = 0; i < nBlocks; i++)
            {
                blockStarts[i] = i * defBlockSize;
                blockSizes[i] = (i == nBlocks - 1) ? (defBlockSize + array.Length % nBlocks) : defBlockSize;
            }

            Thread[] threads = new Thread[nBlocks];
            for (i = 0; i < nBlocks; i++)
            {
                threads[i] = new Thread(SortBlock);
                threads[i].Start(i);
            }

            for (i = 0; i < nBlocks; i++) threads[i].Join();


            int[] sorted = new int[array.Length];

            for (i = 0; i < array.Length; ++i)
            {
                int minIdx = -1;
                for (j = 0; j < nBlocks; ++j)
                {
                    if (blockSizes[j] > 0)
                    {
                        if (minIdx<0 || array[blockStarts[minIdx]] > array[blockStarts[j]])
                        {
                            minIdx = j;
                        }
                    }
                }
                sorted[i] = array[blockStarts[minIdx]];
                ++blockStarts[minIdx];
                --blockSizes[minIdx];
            }


            return sorted;
        }


        private void SortBlock(object param)
        {
            int id = (int)param;
            int myStart = blockStarts[id];
            int mySize = blockSizes[id];
            Array.Sort(arr, myStart, mySize);
        }
    }
}
