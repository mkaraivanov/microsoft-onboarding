#r "Microsoft.WindowsAzure.Storage"
#r "Microsoft.VisualBasic"
#r "System.Data"
#r "D:\home\site\wwwroot\convert-learning-csv-to-json\bin\Newtonsoft.Json.dll"

using System;
using System.Net;
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
using Microsoft.VisualBasic.FileIO;
using Newtonsoft.Json;

public static async Task<HttpResponseMessage> Run(HttpRequestMessage req, TraceWriter log)
{
    log.Info("C# HTTP trigger function processed a request.");

    // parse query parameter
    string name = req.GetQueryNameValuePairs()
        .FirstOrDefault(q => string.Compare(q.Key, "name", true) == 0)
        .Value;

    if (name == null)
    {
        // Get request body
        dynamic data = await req.Content.ReadAsAsync<object>();
        name = data?.name;
    }

    string accessKey;
    string accountName;
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
        return new HttpResponseMessage(HttpStatusCode.BadRequest)
            {
                Content = new StringContent(JsonConvert.SerializeObject("Please pass a valid file name on the query string or in the request body"), Encoding.UTF8, "application/json")
            };
    }

    CultureInfo enUSFormat = new CultureInfo("en-US");

    List<OnboardingPerson> onboardingPeopleList = new List<OnboardingPerson>();

    using (var memoryStream = new MemoryStream())
    {
        await blockBlobReference.DownloadToStreamAsync(memoryStream);

        memoryStream.Position = 0;

        using (TextFieldParser parser = new TextFieldParser(memoryStream))
        {
            parser.CommentTokens = new string[] { "#" };
            parser.SetDelimiters(new string[] { "," });
            parser.HasFieldsEnclosedInQuotes = true;

            // Skip over header lines
            parser.ReadLine();
            parser.ReadLine();
            parser.ReadLine();

            while (!parser.EndOfData)
            {
                string[] fields = parser.ReadFields();

                string strCompletionOrCancellationDate = fields[34];

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

                if (boolCompletionOrCancellationDateConverted == false || dateCompletionOrCancellationDate != DateTime.Today)
                {
                    continue;
                }

                OnboardingPerson onboardingPerson = new OnboardingPerson()
                {
                    StudentID = fields[0],
                    MSAlias = fields[1],
                    EmailAddress = fields[2],
                    FullName = fields[3],
                    ItemID = fields[4],
                    SubjectID = fields[5],
                    ItemTitle = fields[6],
                    ItemType = fields[7],
                    DeliveryMethod = fields[8],
                    HostedTrainingOrganization = fields[9],
                    ScheduleOfferingID = fields[10],
                    StartDate = fields[11],
                    StartTime = fields[12],
                    EndDate = fields[13],
                    EndTime = fields[14],
                    TimeZone = fields[15],
                    Facility = fields[16],
                    FacilityCountry = fields[17],
                    FacilityCity = fields[18],
                    OfferingStatus = fields[19],
                    DeliveryCoordinator = fields[20],
                    SupervisorName = fields[21],
                    SupervisorEmail = fields[22],
                    ManagerOfManagers = fields[23],
                    L2Manager = fields[24],
                    StudentOrganization = fields[25],
                    StudentCountry = fields[26],
                    StudentCity = fields[27],
                    YearsAtMicrosoft = fields[28],
                    StandardTitle = fields[29],
                    Profession = fields[30],
                    Discipline = fields[31],
                    EnrollmentStatusID = fields[32],
                    CompletionStatus = fields[33],
                    CompletionOrCancellationDate = fields[34]
                };

                onboardingPeopleList.Add(onboardingPerson);
            }
        }
    }

    return name == null
        ? new HttpResponseMessage(HttpStatusCode.BadRequest)
        {
            Content = new StringContent(JsonConvert.SerializeObject("Please pass a valid file name on the query string or in the request body"), Encoding.UTF8, "application/json")
        }
        : new HttpResponseMessage(HttpStatusCode.OK)
        {
            Content = new StringContent(JsonConvert.SerializeObject(onboardingPeopleList, Formatting.Indented), Encoding.UTF8, "application/json")
        };
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
    public string CompletionOrCancellationDate { get; set; }
}
