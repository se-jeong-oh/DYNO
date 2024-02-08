// {'CC13': 102.823425, 'CG13': 138.39551, 'GC13': 174.84564, 'GG13': 174.84564, 'CC12': 262.3577, 'CG12': 262.3577, 'GC12': 262.3577, 'GG12': 262.3577, 'CC11': 347.91315, 'CG11': 347.91315, 'GC11': 347.91315, 'GG11': 347.91315, 'CC10': 452.23352, 'CG10': 452.23352, 'GC10': 452.23352, 'GG10': 452.23352, 'CC9': 508.39484, 'CG9': 508.39484, 'GC9': 508.39484, 'GG9': 508.39484, 'CC8': 508.39484, 'CG8': 508.39484, 'GC8': 508.39484, 'GG8': 508.39484, 'CC7': 568.8793, 'CG7': 568.8793, 'GC7': 568.8793, 'GG7': 568.8793, 'CC6': 676.1659, 'CG6': 676.1659, 'GC6': 676.1659, 'GG6': 676.1659, 'CC5': 734.54364, 'CG5': 734.54364, 'GC5': 734.54364, 'GG5': 734.54364, 'CC4': 794.1743, 'CG4': 794.1743, 'GC4': 794.1743, 'GG4': 794.1743, 'CC3': 856.3833, 'CG3': 856.3833, 'GC3': 856.3833, 'GG3': 856.3833, 'CC2': 921.1337, 'CG2': 921.1337, 'GC2': 921.1337, 'GG2': 921.1337, 'CC1': 991.0982, 'CG1': 991.0982, 'GC1': 991.0982, 'GG1': 991.0982, 'CC0': 991.0982, 'CG0': 991.0982, 'GC0': 991.0982, 'GG0': 991.0982}
// Map(HashAggregateExec -> 7, ExpandExec -> 10, ProjectExec -> 11, StateStoreSaveExec -> 2, StateStoreRestoreExec -> 4, FilterExec -> 12, ShuffleExchangeExec -> 6, FileSourceScanExec -> 13)

object ShortestPathAlgorithm {
    def main(args: Array[String]): Unit = {
        val estimatedExecMapString = "GG5 -> 714.37274, GG8 -> 489.79376, GG12 -> 246.88739, CG12 -> 246.88739, CG3 -> 835.6116, GG2 -> 900.11255, GC10 -> 434.1903, GC9 -> 489.79376, CC10 -> 434.1903, CG6 -> 656.3346, CG9 -> 489.79376, CC7 -> 549.7862, CC13 -> 91.937195, GC13 -> 161.49594, CG0 -> 969.86786, GC3 -> 835.6116, GC6 -> 656.3346, CC1 -> 969.86786, GC0 -> 969.86786, CC4 -> 773.6885, GG4 -> 773.6885, GC12 -> 246.88739, CG8 -> 489.79376, CG11 -> 331.05426, GG11 -> 331.05426, GG7 -> 549.7862, CG2 -> 900.11255, GG1 -> 969.86786, CG5 -> 714.37274, CC3 -> 835.6116, GC8 -> 489.79376, GC2 -> 900.11255, CC12 -> 246.88739, CC6 -> 656.3346, CC0 -> 969.86786, CC9 -> 489.79376, GC5 -> 714.37274, GG13 -> 161.49594, CG4 -> 773.6885, GG9 -> 489.79376, GG0 -> 969.86786, CC11 -> 331.05426, CG7 -> 549.7862, GC11 -> 331.05426, GG3 -> 835.6116, GG6 -> 656.3346, GC4 -> 773.6885, CG10 -> 434.1903, CG1 -> 969.86786, CC8 -> 489.79376, GG10 -> 434.1903, CC2 -> 900.11255, CG13 -> 126.17556, GC7 -> 549.7862, GC1 -> 969.86786, CC5 -> 714.37274"
        val estimatedExecMapPairs = estimatedExecMapString.split(",")
        val estimatedExecMap = estimatedExecMapPairs.map { pair =>
            val keyValue = pair.trim.split(" -> ")
            if (keyValue.length == 2) {
                keyValue(0) -> keyValue(1).toFloat
            } else {
                // Handle cases where the input format is incorrect
                throw new IllegalArgumentException("Invalid input format")
            }
        }.toMap

        // println(estimatedExecMap)
        val shortestPath = runShortestPath(estimatedExecMap)
        println(s"Shortest Path is : $shortestPath")
    }

    def runShortestPath(estimatedExecMap : Map[String, Float]): String = {
        // 0th operator only can use CC or CG 
        // CC : previous CPU - current CPU
        var id = 0
        // In order of sequence, CC / GC / CG / GG
        // Index of these are -> 0  / 1  / 2  / 3
        var pathLength : Array[Double] = Array(Double.PositiveInfinity, Double.PositiveInfinity, Double.PositiveInfinity, Double.PositiveInfinity)
        var tempLength : Array[Double] = Array(Double.PositiveInfinity, Double.PositiveInfinity, Double.PositiveInfinity, Double.PositiveInfinity)
        var path : Array[String] = Array("", "", "", "")
        var temp : Array[String] = Array("", "", "", "")
        while(estimatedExecMap.contains("CC"+id)) {
            id += 1
        }
        id -= 1
        val maxId = id

        while(id >= 0) {
            if (id == maxId) {
                tempLength(0) = estimatedExecMap("CC"+id)
                temp(0) = "CC"+id
                tempLength(2) = estimatedExecMap("CG"+id)
                temp(2) = "CG"+id
            }   
            else {
                // in CC case, compare CC / GC
                // in GC case, compare CG / GG
                if (pathLength(0) < pathLength(1)) {
                    tempLength(0) = pathLength(0) + estimatedExecMap("CC"+id)
                    temp(0) = path(0)+","+"CC"+id
                    tempLength(2) = pathLength(0) + estimatedExecMap("CG"+id)
                    temp(2) = path(0)+","+"CG"+id
                }
                // in CC case, compare CC / GC
                // in GC case, compare CG / GG
                else {
                    tempLength(0) = pathLength(1) + estimatedExecMap("CC"+id)
                    temp(0) = path(1)+","+"CC"+id
                    tempLength(2) = pathLength(1) + estimatedExecMap("CG"+id)
                    temp(2) = path(1)+","+"CG"+id
                }
                // in CG case, compare CC / GC
                // in GG case, compare CG / GG
                if (pathLength(2) < pathLength(3)) {
                    tempLength(1) = pathLength(2) + estimatedExecMap("GC"+id)
                    temp(1) = path(2)+","+"GC"+id
                    tempLength(3) = pathLength(2) + estimatedExecMap("GG"+id)
                    temp(3) = path(2)+","+"GG"+id
                }
                // in CG case, compare CC / GC
                // in GG case, compare CG / GG
                else {
                    tempLength(1) = pathLength(3) + estimatedExecMap("GC"+id)
                    temp(1) = path(3)+","+"GC"+id
                    tempLength(3) = pathLength(3) + estimatedExecMap("GG"+id)
                    temp(3) = path(3)+","+"GG"+id
                }
                /*
                tempLength(0) = math.min(pathLength(0), pathLength(1)) + estimatedExecMap("CC"+id)
                temp(0) += path(0)+","+"CC"+id
                // in GC case, compare CG / GG
                tempLength(1) = math.min(pathLength(2), pathLength(3)) + estimatedExecMap("GC"+id)
                // in CG case, compare CC / GC
                tempLength(2) = math.min(pathLength(0), pathLength(1)) + estimatedExecMap("CG"+id)
                // in GG case, compare CG / GG
                tempLength(3) = math.min(pathLength(2), pathLength(3)) + estimatedExecMap("GG"+id)
                */
            }
            pathLength = tempLength.clone()
            path = temp.clone()
            id -= 1
        }
        val shortestPathLength = pathLength.reduce((x, y) => if (x < y) x else y)
        val shortestPathIndex = pathLength.indexOf(shortestPathLength)

        // println(pathLength)
        println(s"The minimum value is: ${shortestPathLength}")
        path(shortestPathIndex)
    }
}