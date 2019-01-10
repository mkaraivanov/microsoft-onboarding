#r "Microsoft.WindowsAzure.Storage"
#r "System.Data"
#r "Newtonsoft.Json"
#r "D:\home\site\wwwroot\convert-learning-csv-to-json\bin\CsvHelper.dll"

using System.Net;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Primitives;
using Newtonsoft.Json;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Data;
using System.Reflection;
using System.Globalization;
using Microsoft.WindowsAzure.Storage;
using Microsoft.Azure;
using Microsoft.WindowsAzure.Storage.Blob;
using CsvHelper;

public static async Task<IActionResult> Run(HttpRequest req, ILogger log)
{
    log.LogInformation("C# HTTP trigger function processed a request.");

    string name = req.Query["name"];
    string operationDate = req.Query["date"];

    string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
    dynamic data = JsonConvert.DeserializeObject(requestBody);
    name = name ?? data?.name;
    operationDate = operationDate ?? data?.date;

    CultureInfo enUSFormat = new CultureInfo("en-US");

    DateTime dateOperationDate = DateTime.UtcNow; 
    if (string.IsNullOrEmpty(operationDate) == false)
    {
        if (operationDate.Length >= 10)
        {
            operationDate = operationDate.Substring(0, 10).Trim();
        }

        DateTime.TryParse(operationDate, enUSFormat, DateTimeStyles.None, out dateOperationDate);
    }

    string accessKey;
    string connectionString;
    string containerName;
    CloudStorageAccount storageAccount;

    accessKey = System.Environment.GetEnvironmentVariable("AzureStorageAccessKey");
    connectionString = System.Environment.GetEnvironmentVariable("AzureStorageConnectionString");
    storageAccount = CloudStorageAccount.Parse(connectionString);

    CloudBlobClient client;
    CloudBlobContainer container;

    client = storageAccount.CreateCloudBlobClient();

    containerName = System.Environment.GetEnvironmentVariable("AzureStorageContainerName");
    container = client.GetContainerReference(containerName);

    CloudBlockBlob blockBlobReference;

    try
    {
        blockBlobReference = container.GetBlockBlobReference(name);
    }
    catch (Exception ex)
    {
        return new BadRequestObjectResult("Please pass a valid file name on the query string or in the request body");
    }

    List<OnboardingPerson> onboardingPeopleList = new List<OnboardingPerson>();
    
    using (var memoryStream = new MemoryStream())
    {
        await blockBlobReference.DownloadToStreamAsync(memoryStream);

        memoryStream.Position = 0;

        TextReader textReader = new StreamReader(memoryStream);

        var csv = new CsvReader(textReader);
        csv.Configuration.HasHeaderRecord = true;
        csv.Configuration.PrepareHeaderForMatch = header => header.Trim().Replace(" ", string.Empty).Replace("/", string.Empty).ToLower();
        csv.Configuration.Delimiter = ",";
        csv.Configuration.IgnoreBlankLines = true;
        //csv.Configuration.IgnoreQuotes = false;
        //csv.Configuration.Quote = '"';
        csv.Configuration.Comment = '#';
        csv.Configuration.MissingFieldFound = null;

        csv.Read();
        csv.Read();
        csv.Read();
        bool headerRead = csv.ReadHeader();
        while (csv.Read())
        {
            var record = csv.GetRecord<OnboardingPerson>();

            string strCompletionOrCancellationDate = record.CompletionDateCancellationDate;

            if (string.IsNullOrEmpty(strCompletionOrCancellationDate))
            {
                continue;
            }

            if (strCompletionOrCancellationDate.Length >= 10)
            {
                strCompletionOrCancellationDate = strCompletionOrCancellationDate.Substring(0, 10).Trim();
            }

            DateTime dateCompletionOrCancellationDate;

            bool boolCompletionOrCancellationDateConverted = DateTime.TryParse(
                strCompletionOrCancellationDate, enUSFormat,
                DateTimeStyles.None,
                out dateCompletionOrCancellationDate);

            if (boolCompletionOrCancellationDateConverted && dateCompletionOrCancellationDate == dateOperationDate)
            {
                onboardingPeopleList.Add(record);
            }
        }
    }

    return name != null
        ? (ActionResult)new OkObjectResult($"{JsonConvert.SerializeObject(onboardingPeopleList)}")
        : new BadRequestObjectResult("Please pass a name on the query string or in the request body");
}

public class OnboardingPerson
{
    public string StudentID { get; set; }
    public string MSAlias { get; set; }
    public string EmailAddress { get; set; }
    public string FullName { get; set; }
    public string ItemID { get; set; }
    public string SubjectID { get; set; }
    public string ItemTitle { get; set; }
    public string ItemType { get; set; }
    public string DeliveryMethod { get; set; }
    public string HostedTrainingOrganization { get; set; }
    public string ScheduleOfferingID { get; set; }
    public string StartDate { get; set; }
    public string StartTime { get; set; }
    public string EndDate { get; set; }
    public string EndTime { get; set; }
    public string TimeZone { get; set; }
    public string Facility { get; set; }
    public string FacilityCountry { get; set; }
    public string FacilityCity { get; set; }
    public string OfferingStatus { get; set; }
    public string DeliveryCoordinator { get; set; }
    public string SupervisorName { get; set; }
    public string SupervisorEmail { get; set; }
    public string ManagerOfManagers { get; set; }
    public string L2Manager { get; set; }
    public string StudentOrganization { get; set; }
    public string StudentCountry { get; set; }
    public string StudentCity { get; set; }
    public string YearsAtMicrosoft { get; set; }
    public string StandardTitle { get; set; }
    public string Profession { get; set; }
    public string Discipline { get; set; }
    public string EnrollmentStatusID { get; set; }
    public string CompletionStatus { get; set; }
    public string CompletionDateCancellationDate { get; set; }
}
