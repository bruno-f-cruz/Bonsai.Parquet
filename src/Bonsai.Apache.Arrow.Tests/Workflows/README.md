# Arrow and Parquet workflow tests

Set `Bonsai.Apache` as the startup project in Visual Studio and press F5 to open
`api-tests.bonsai`. Press **Start** to run all nine examples. Each group returns
a boolean; **Summary** is true when all pass.

Open a group to inspect the writers and readers. Use a reader's **Columns**
editor to try **Load Schema**. Output files go to `artifacts/api-tests/`.

CI runs this workflow automatically. To run its tests locally:

```powershell
dotnet test src/Bonsai.Apache.Arrow.Tests --framework net8.0 --filter TestCategory=Workflow
```
