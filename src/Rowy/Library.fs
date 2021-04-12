namespace Rowy

open FSharp.Stats

type Table<'R,'C,'V when 'R : equality and 'C : equality> = 
    {
    RowKeys : 'R []
    ColumnKeys : 'C []
    Rows : 'V [][]
    }

    member this.RowCount = this.RowKeys.Length
    member this.ColumnCount = this.ColumnKeys.Length

    static member create rowKeys columnKeys rows =
        {
            RowKeys = rowKeys
            ColumnKeys = columnKeys
            Rows = rows
        }

    static member getRowKeys (dataFrame : Table<'R,'C,'V>) = dataFrame.RowKeys
    static member getColumnKeys (dataFrame : Table<'R,'C,'V>) = dataFrame.ColumnKeys
    static member getRows (dataFrame : Table<'R,'C,'V>) = dataFrame.Rows

    static member getRowCount (dataFrame : Table<'R,'C,'V>) = dataFrame.RowCount
    static member getColumnCount (dataFrame : Table<'R,'C,'V>) = dataFrame.ColumnCount

    static member fromFile (path:string,f:string->'V,?HasHeaders:bool,?HasRowKeys:bool,?Separator:char) =
        let hasHeaders = Option.defaultValue true HasHeaders
        let hasRowKeys = Option.defaultValue true HasRowKeys
        let separator = Option.defaultValue '\t' Separator

        let r = new System.IO.StreamReader(path)

        let columnKeys = 
            if hasHeaders then
                let line = r.ReadLine().Split(separator)
                if hasRowKeys then 
                    line |> Array.skip 1
                else line
            else 
                [||]

        let rowKeys,rows =
            if hasRowKeys then 
                let rowKeys = System.Collections.Generic.HashSet<string>()
                let rows = 
                    [|
                    while not r.EndOfStream do
                        let line = r.ReadLine().Split(separator)
                        if not (rowKeys.Add(line.[0])) then failwithf "duplicate key %s" line.[0]
                        [|
                        for i = 1 to line.Length - 1 do
                            f line.[i]
                        |]
                    |]
                rowKeys |> Seq.toArray,rows
            else
                [||],
                [|
                    while not r.EndOfStream do
                        let line = r.ReadLine().Split(separator)
                        [|
                        for i = 0 to line.Length - 1 do
                            f line.[i]
                        |]
                    |]
        r.Close()
        Table<'R,'C,'V>.create
            rowKeys
            columnKeys
            rows

    static member toFile (path:string,f:'V->string,df:Table<string,string,'V>,?IncludeHeaders:bool,?IncludeRowKeys:bool,?Separator:char) =
        let includeHeaders = 
            match IncludeHeaders with
            | Some true when df.ColumnCount >= 1 -> true
            | None when df.ColumnCount >= 1 -> true
            | _ -> false
        let includeRowKeys = 
            match IncludeRowKeys with
            | Some true when df.RowCount >= 1 -> true
            | None when df.RowCount >= 1 -> true
            | _ -> false
        let separator = Option.defaultValue '\t' Separator |> string

        let w = new System.IO.StreamWriter(path)

        if includeHeaders then
            let line = 
                if includeRowKeys then
                    "Row Keys" + separator + Array.reduce (fun a b -> a + separator + b) df.ColumnKeys
                else
                    Array.reduce (fun a b -> a + separator + b) df.ColumnKeys
            w.WriteLine(line)
            w.Flush()
        if includeRowKeys then
            (df.RowKeys,df.Rows)
            ||> Array.iter2 (fun rowKey row ->
                w.Write(rowKey)
                row |> Array.iter (fun v -> w.Write(separator + (string v)))
                w.WriteLine()
            )
        else
            df.Rows
            |> Array.iter (fun  row ->
                row |> Array.iter (fun v -> w.Write(separator + (string v)))
                w.WriteLine()
            )
        
        w.Close()


module Table =

    let transpose (dataFrame : Table<'R,'C,'V>) : Table<'C,'R,'V>=
        let transposedFrame = dataFrame.Rows |> JaggedArray.transpose
        Table<'C,'R,'V>.create
            dataFrame.ColumnKeys
            dataFrame.RowKeys
            transposedFrame

    let filterRows (f : 'R -> 'V [] -> bool) (dataFrame : Table<'R,'C,'V>) : Table<'R,'C,'V> =
        let filteredRowKeys,filteredFrame = 
            Array.zip dataFrame.RowKeys dataFrame.Rows
            |> Array.filter (fun (r,vs) -> f r vs)
            |> Array.unzip       
        Table<'R,'C,'V>.create
            filteredRowKeys
            dataFrame.ColumnKeys
            filteredFrame

    let filterRowValues (f : 'V [] -> bool) (dataFrame : Table<'R,'C,'V>) : Table<'R,'C,'V> =
        let indices = System.Collections.Generic.HashSet<int>()
        let filteredFrame = 
            dataFrame.Rows
            |> Array.indexed
            |> Array.choose (fun (i,row) ->
                let b = f row
                if b then 
                    Some row
                else 
                    indices.Add(i) |> ignore
                    None
            )
        let filteredRowKeys = 
            dataFrame.RowKeys
            |> Array.indexed
            |> Array.choose (fun (i,k) ->
                if indices.Contains i then 
                    None
                else Some k 
            )
        Table<'R,'C,'V>.create
            filteredRowKeys
            dataFrame.ColumnKeys
            filteredFrame

    let filterCols (f : 'C -> 'V [] -> bool) (dataFrame : Table<'R,'C,'V>) : Table<'R,'C,'V> =
        transpose dataFrame |> filterRows f |> transpose

    let filterColValues (f : 'V [] -> bool) (dataFrame : Table<'R,'C,'V>) : Table<'R,'C,'V> =
        transpose dataFrame |> filterRowValues f |> transpose

    let map (f : 'V -> 'VR) (dataFrame : Table<'R,'C,'V>) =
        dataFrame.Rows
        |> JaggedArray.map f
        |> Table<'R,'C,'V>.create dataFrame.RowKeys dataFrame.ColumnKeys

    //let mapData (f : 'V [][] -> 'VR [][]) (dataFrame : Table<'R,'C,'V>) =
    //    {dataFrame with Rows = f dataFrame.Rows}