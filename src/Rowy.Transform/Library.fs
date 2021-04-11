namespace Rowy.Transform

open Rowy
open Microsoft.ML
open Microsoft.ML.Data
open Deedle
open FSharp.Stats

type MLInput = 
    {
        Label : float32;
        [<VectorType>]
        FeaturesRaw : float32 array
    } 

    static member toDataView (mlContext : MLContext) (input : MLInput []) =       
        let schema = 
            let schema = SchemaDefinition.Create(typeof<MLInput>)
            let vectorLength = Array.head input |> fun v -> v.FeaturesRaw.Length
            schema.[1].ColumnType <- VectorDataViewType(NumberDataViewType.Single,vectorLength)
            schema
        
        mlContext.Data.LoadFromEnumerable(input, schema)


module Deedle =

   
    let inline fromFrame<'R,'C,'V when 'R : equality and 'C : equality and 'V : equality>(frame:Frame<'R,'C>) =
        Table<'R,'C,'V>.create
            (frame.RowKeys |> Seq.toArray)
            (frame.ColumnKeys |> Seq.toArray)
            (frame.ToArray2D<'V>() |> JaggedArray.ofArray2D)
    
module ML = 

    let toMLRows (labelColumn : 'C) (dataFrame : Table<'R,'C,float32>) =
        let columnIndex = dataFrame.ColumnKeys |> Array.tryFindIndex ((=) labelColumn)
        match columnIndex with
        | Some index ->       
            let length = dataFrame.ColumnKeys.Length
            dataFrame.Rows
            |> Array.map (fun row ->
                {
                    Label = row.[index]
                    FeaturesRaw = Array.init (length-1) (fun i -> if i >= index then row.[i+1] else row.[i])
                }
            )

        | None -> failwithf "Column Header %A does not exist in the frame" labelColumn

    let toDataView (context: MLContext) (labelColumn : 'C) (table:Table<'R,'C,float32>) =

        toMLRows labelColumn table
        |> MLInput.toDataView context