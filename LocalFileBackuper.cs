using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using Microsoft.Extensions.Logging;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace FunctionParallelDemo;

public static class LocalFileBackuper
{
    [FunctionName("FunctionDemoOrchestrator")]
    public static async Task<long> Orchestrate(
        [OrchestrationTrigger] IDurableOrchestrationContext backupContext)
    {
        string rootDirectory = backupContext.GetInput<string>()?.Trim();
        if (string.IsNullOrEmpty(rootDirectory))
        {
            rootDirectory = Directory.GetParent(typeof(LocalFileBackuper).Assembly.Location).FullName;
        }

        string[] files = await backupContext.CallActivityAsync<string[]>(
            "FunctionDemo_GetFiles",
            rootDirectory);

        var tasks = new Task<long>[files.Length];
        for (int i = 0; i < files.Length; i++)
        {
            tasks[i] = backupContext.CallActivityAsync<long>(
                "FunctionDemo_UploadToBlob",
                files[i]);
        }

        await Task.WhenAll(tasks);

        long totalBytes = tasks.Sum(t => t.Result);
        return totalBytes;
    }

    [FunctionName("FunctionDemo_GetFiles")]
    public static string[] GetFiles(
        [ActivityTrigger] string rootDirectory,
        ILogger log)
    {
        log.LogInformation($"Searching for files under '{rootDirectory}'...");
        string[] files = Directory.GetFiles(rootDirectory, "*", SearchOption.AllDirectories);
        log.LogInformation($"Found {files.Length} file(s) under {rootDirectory}.");

        return files;
    }

    [FunctionName("FunctionDemo_CreateBackup")]
    public static async Task<long> CreateBackup(
        [ActivityTrigger] string filePath,
        ILogger log)
    {
        long byteCount = new FileInfo(filePath).Length;

        // strip the drive letter prefix and convert to forward slashes

        var pathRoot = Path.GetPathRoot(filePath);
        var backupPath = pathRoot + "backup." + filePath[pathRoot.Length..]
            .Replace('\\', '/');

        log.LogInformation($"Copying '{filePath}' to '{backupPath}'. Total bytes = {byteCount}.");

        try
        {
            var task = Task.Run(() => File.Copy(filePath, backupPath, true));
            await task;
        }
        catch (IOException iox)
        {
            log.LogError(iox.Message);
        }

        return byteCount;
    }
}
