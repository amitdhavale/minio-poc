using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Minio;
using Minio.DataModel;
using Minio.Exceptions;

namespace MinioPOC.Controllers
{
    [Route("api/[controller]/[action]")]
    [ApiController]
    public class MinioController : ControllerBase
    {
        private readonly IConfiguration _config;

        public MinioController(IConfiguration config)
        {
            _config = config;
        }

        /// <summary>
        /// List all buckets
        /// </summary>
        /// <returns></returns>
        [HttpGet]
        public async Task<ActionResult> ListBuckets()
        {
            try
            {
                MinioClient minioClient = new MinioClient(_config["Minio:DefaultConnectionString"], _config["minio:Username"], _config["minio:Password"]);
                
                var bucketList = await minioClient.ListBucketsAsync();

                return StatusCode(StatusCodes.Status200OK, bucketList);
            }
            catch(Exception ex)
            {
                return StatusCode(StatusCodes.Status500InternalServerError, ex.ToString());
            }
        }

        /// <summary>
        /// List incomplete uploads in bucket
        /// </summary>
        /// <param name="bucketName"></param>
        /// <returns></returns>
        [HttpGet]
        public async Task<ActionResult> ListIncompleteUploadsInBucket(string bucketName)
        {
            try
            {
                MinioClient minioClient = new MinioClient(_config["Minio:DefaultConnectionString"], _config["minio:Username"], _config["minio:Password"]);

                var observableObjectList = minioClient.ListIncompleteUploads(bucketName);

                return StatusCode(StatusCodes.Status200OK, observableObjectList);
            }
            catch (Exception ex)
            {
                return StatusCode(StatusCodes.Status500InternalServerError, ex.ToString());
            }
        }

        /// <summary>
        /// List Objects In Bucket 
        /// </summary>
        /// <param name="bucketName"></param>
        /// <returns></returns>
        [HttpGet]
        public async Task<ActionResult> ListObjects(string bucketName)
        {
            try
            {
                MinioClient minioClient = new MinioClient(_config["Minio:DefaultConnectionString"], _config["minio:Username"], _config["minio:Password"]);

                var objectList = minioClient.ListObjectsAsync(bucketName);

                return StatusCode(StatusCodes.Status200OK, objectList);
            }
            catch (Exception ex)
            {
                return StatusCode(StatusCodes.Status500InternalServerError, ex.ToString());
            }
        }

        /// <summary>
        /// Get Object Metadata
        /// </summary>
        /// <param name="bucketName"></param>
        /// <param name="objectName"></param>
        /// <returns></returns>
        [HttpGet]
        public async Task<ActionResult> ListObjectMetadata(string bucketName, string objectName)
        {
            try
            {
                MinioClient minioClient = new MinioClient(_config["Minio:DefaultConnectionString"], _config["minio:Username"], _config["minio:Password"]);

                ObjectStat objectStat = await minioClient.StatObjectAsync(bucketName, objectName);

                return StatusCode(StatusCodes.Status200OK, objectStat);
            }
            catch (Exception ex)
            {
                return StatusCode(StatusCodes.Status500InternalServerError, ex.ToString());
            }
        }

        /// <summary>
        /// Get Object In Stream
        /// </summary>
        /// <param name="bucketName"></param>
        /// <param name="objectName"></param>
        /// <returns></returns>
        [HttpGet]
        public async Task<ActionResult> GetObjectAsFile(string bucketName, string objectName)
        {
            try
            {
                MinioClient minioClient = new MinioClient(_config["Minio:DefaultConnectionString"], _config["minio:Username"], _config["minio:Password"]);

                MemoryStream fileStream = new MemoryStream();
                try
                {
                    ObjectStat objectStat = await minioClient.StatObjectAsync(bucketName, objectName);

                    await minioClient.GetObjectAsync(bucketName, objectName,
                        (stream) =>
                        {
                            stream.CopyTo(fileStream);
                        });

                    byte[] fileContent = fileStream.ToArray();

                    return File(fileContent, objectStat.ContentType, objectStat.ObjectName);
                }
                catch (MinioException ex)
                {
                    return StatusCode(StatusCodes.Status500InternalServerError, ex.ToString());
                }
            }
            catch (Exception ex)
            {
                return StatusCode(StatusCodes.Status500InternalServerError, ex.ToString());
            }
        }

        /// <summary>
        /// Create a bucket if it doesnt exist
        /// </summary>
        /// <param name="bucketName"></param>
        /// <returns></returns>
        [HttpPost]
        public async Task<ActionResult> CreateBucket(string bucketName)
        {
            try
            {
                string result = "Bucket already exists."; 

                MinioClient minioClient = new MinioClient(_config["Minio:DefaultConnectionString"], _config["minio:Username"], _config["minio:Password"]);
                
                bool bucketFound = await minioClient.BucketExistsAsync(bucketName);

                if (bucketFound == false)
                {
                    await minioClient.MakeBucketAsync(bucketName);
                    result = "Bucket created.";
                }

                return StatusCode(StatusCodes.Status200OK, result);
            }
            catch (Exception ex)
            {
                return StatusCode(StatusCodes.Status500InternalServerError, ex.ToString());
            }
        }

        /// <summary>
        /// Post Object To Bucket
        /// </summary>
        /// <param name="bucketName"></param>
        /// <param name="objectName"></param>
        /// <param name="file"></param>
        /// <returns></returns>
        [HttpPost]
        public async Task<ActionResult> CreateObject([FromQuery] string bucketName, string objectName, IFormFile file)
        {
            try
            {
                MinioClient minioClient = new MinioClient(_config["Minio:DefaultConnectionString"], _config["minio:Username"], _config["minio:Password"]);

                MemoryStream memoryStream = new MemoryStream();

                file.CopyTo(memoryStream);

                await minioClient.PutObjectAsync(bucketName, objectName, memoryStream, file.Length, file.ContentType);

                return StatusCode(StatusCodes.Status200OK, "Object created.");
            }
            catch (Exception ex)
            {
                return StatusCode(StatusCodes.Status500InternalServerError, ex.ToString());
            }
        }

        /// <summary>
        /// Copy object from a bucket to another object in different bucket
        /// </summary>
        /// <param name="sourceBucket"></param>
        /// <param name="sourceObject"></param>
        /// <param name="destinationBucket"></param>
        /// <param name="destinationObject"></param>
        /// <returns></returns>
        [HttpPost]
        public async Task<ActionResult> CopyObject([FromQuery] string sourceBucket, string sourceObject, string destinationBucket, string destinationObject)
        {
            try
            {
                MinioClient minioClient = new MinioClient(_config["Minio:DefaultConnectionString"], _config["minio:Username"], _config["minio:Password"]);

                if (await minioClient.BucketExistsAsync(sourceBucket) == false)
                {
                    return StatusCode(StatusCodes.Status400BadRequest, $"Bucket {sourceBucket} does not exist.");
                }
                if (await minioClient.BucketExistsAsync(destinationBucket) == false)
                {
                    return StatusCode(StatusCodes.Status400BadRequest, $"Bucket {sourceBucket} does not exist.");
                }

                await minioClient.CopyObjectAsync(sourceBucket, sourceObject, destinationBucket, destinationObject);

                return StatusCode(StatusCodes.Status200OK, "Object created.");
            }
            catch (Exception ex)
            {
                return StatusCode(StatusCodes.Status500InternalServerError, ex.ToString());
            }
        }


        /// <summary>
        /// Remove single object from bucket
        /// </summary>
        /// <param name="bucketName"></param>
        /// <param name="objectName"></param>
        /// <returns></returns>
        [HttpDelete]
        public async Task<ActionResult> RemoveObject([FromQuery] string bucketName, string objectName)
        {
            try
            {
                MinioClient minioClient = new MinioClient(_config["Minio:DefaultConnectionString"], _config["minio:Username"], _config["minio:Password"]);

                await minioClient.RemoveObjectAsync(bucketName, objectName);

                return StatusCode(StatusCodes.Status200OK, "Object removed.");
            }
            catch (Exception ex)
            {
                return StatusCode(StatusCodes.Status500InternalServerError, ex.ToString());
            }
        }

        /// <summary>
        /// Removes list of objects from bucket
        /// </summary>
        /// <param name="bucketName"></param>
        /// <param name="objectList"></param>
        /// <returns></returns>
        [HttpDelete]
        public async Task<ActionResult> RemoveListOfObjectsFromBucket([FromQuery] string bucketName, List<string> objectList)
        {
            try
            {
                MinioClient minioClient = new MinioClient(_config["Minio:DefaultConnectionString"], _config["minio:Username"], _config["minio:Password"]);

                await minioClient.RemoveObjectAsync(bucketName, objectList);

                return StatusCode(StatusCodes.Status200OK, "Object removed.");
            }
            catch (Exception ex)
            {
                return StatusCode(StatusCodes.Status500InternalServerError, ex.ToString());
            }
        }

        /// <summary>
        /// Removes partially uploaded object
        /// </summary>
        /// <param name="bucketName"></param>
        /// <param name="objectName"></param>
        /// <returns></returns>
        [HttpDelete]
        public async Task<ActionResult> RemoveIncompleteObjectsFromBucket([FromQuery] string bucketName, string objectName)
        {
            try
            {
                MinioClient minioClient = new MinioClient(_config["Minio:DefaultConnectionString"], _config["minio:Username"], _config["minio:Password"]);

                await minioClient.RemoveIncompleteUploadAsync(bucketName, objectName);

                return StatusCode(StatusCodes.Status200OK, "Object removed.");
            }
            catch (Exception ex)
            {
                return StatusCode(StatusCodes.Status500InternalServerError, ex.ToString());
            }
        }

    }
}
