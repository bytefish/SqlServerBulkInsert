// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using NUnit.Framework;
using System;
using System.Diagnostics;

namespace SqlServerBulkInsert.Test.Measurement
{
    public static class MeasurementUtils
    {
        public static void MeasureElapsedTime(string description, Action action)
        {
            // Get the elapsed time as a TimeSpan value.
            TimeSpan ts = MeasureElapsedTime(action);

            // Format and display the TimeSpan value.
            string elapsedTime = String.Format("{0:00}:{1:00}:{2:00}.{3:00}",
                ts.Hours, ts.Minutes, ts.Seconds,
                ts.Milliseconds / 10);

            TestContext.WriteLine("[{0}] Elapsed Time = {1}", description, elapsedTime);
        }

        private static TimeSpan MeasureElapsedTime(Action action)
        {
            Stopwatch stopWatch = new Stopwatch();
            stopWatch.Start();
            action();
            stopWatch.Stop();

            return stopWatch.Elapsed;
        }

    }
}
