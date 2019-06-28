let _ = require('lodash');
let snowflakeCon = require('../../misc/snowflakeconnection');
let s3Con = require('../../misc/s3connection');
let boxCon = require('../../misc/boxconnection');
let creds = require('../../misc/credentials');
let server = require('../../server/server');

module.exports = function (exportdata) {

  //check servicenowid and guid unique
  exportdata.beforeRemote('create', function (ctx, instance, next) {
    exportdata.count({ servicenowId: ctx.req.body.servicenowId }, (err, count) => {
      if (count > 0)
        return errorCallback('servicenowId already exists', next)
      else {
        exportdata.count({ guid: ctx.req.body.guid }, (err, count) => {
          if (count > 0) {
            return errorCallback('guid already exists', next)
          }
          next()
        })
      }
    });
  })

  exportdata.beforeRemote('replaceById', function (ctx, modelInstance, next) {
    exportdata.find({ where: { servicenowId: ctx.req.body.servicenowId, id: { neq: ctx.req.body.id } } }).then(exportDataValue => {
      if (!_.isEmpty(exportDataValue)) {
        return errorCallback('servicenowId already exists', next)
      }
      exportdata.find({ where: { guid: ctx.req.body.guid, id: { neq: ctx.req.body.id } } }).then(exportDataValue => {
        if (!_.isEmpty(exportDataValue)) {
          return errorCallback('guid already exists', next)
        }
        next()
      })
    });
  })
  // List api to display all records and universal serach for service now id,guid,email,upmid
  exportdata.list = function (filter, callback) {
    if (filter != null && filter.where != null && filter.where.name != null) {
      let name = "%" + filter.where.name + "%";
      exportdata.find({
        where: { or: [{ servicenowId: { ilike: name } }, { email: { ilike: name } }, { upmId: { ilike: name } }, { guid: { ilike: name } }] }
      }).then(data => {
        callback(null, data)
      }).catch(err => {
        return errorCallback(err, callback)
      })
    } else {
      exportdata.find().then(exportList => {
        callback(null, exportList)
      }).catch(err => {
        return errorCallback(err, callback)
      })
    }
  }

  exportdata.remoteMethod('list', {
    http: { path: '/list', verb: 'get' },
    accepts: { arg: 'filter', type: 'object' },
    returns: { arg: 'list', type: 'Object' }
  });

  //omit folder id
  exportdata.afterRemote('find', function (ctx, instance, next) {
    try {
      if (ctx.result != null) {
        ctx.result = _.pick(ctx.result, ['id', 'servicenowId', 'email', 'upmId', 'guid', 'status', 'createdAt', 'updatedAt', 'createdBy', 'updatedBy']);
      }
      next();
    } catch (err) {
      return errorCallback(err, next)
    }
  })

  // run method to export data to box/s3
  exportdata.run = function (id, uploadType, req, callback) {
    req.setTimeout(0);
    let exportMessageLst = []
    exportdata.findById(id).then(exportDataValue => {
      if (_.isEmpty(exportDataValue))
        return errorCallback('Export request not submitted: Invalid id.', callback)
      server.models.admin.find({ where: { status: { neq: null } } }).then(adminLst => {
        if (_.isEmpty(adminLst))
          return errorCallback('Export request not submitted: Invalid admin data.', callback)
        else {
          if (!_.isEqual(exportDataValue.status, 'ACTIVE'))
            return errorCallback('Invalid status.', callback)
          exportDataValue.exportStatus = 'Submitted.'
          exportdata.upsert(exportDataValue).then(() => {
            callback(null, { msg: 'Export request submitted.' })
          }).catch(err => {
            return errorCallback(err, callback)
          })
        }
        let adminData = adminLst[0]
        getCampaignEmailData(exportDataValue.email, adminData, (err, emailCampaignJson) => {
          if (err)
            exportMessageLst.push({ field: 'email_campaign.json', value: 'Export Failed:' + err, timestamp: new Date() })
          getSnkrsEmailData(exportDataValue.upmId, adminData, (err, snkrsLaunchJson) => {
            if (err)
              exportMessageLst.push({ field: 'snkrs_campaign.json', value: 'Export Failed:' + err, timestamp: new Date() })
            if (_.isEmpty(uploadType) || _.isEqual(uploadType, 'S3')) {
              uploadDataToS3(exportDataValue, adminData.exportS3BaseLocation, emailCampaignJson, snkrsLaunchJson, exportMessageLst)
            } else {
              uploadDataToBox(exportDataValue, adminData.exportBoxBaseLocation, emailCampaignJson, snkrsLaunchJson, exportMessageLst)
            }
          })
        })
      }).catch(err => {
        return errorCallback(err, callback)

      })
    }).catch(err => {
      return errorCallback(err, callback)
    })
  }

  // upload data to s3 location
  function uploadDataToS3(exportDataValue, s3Location, emailCampaignJson, snkrsLaunchJson, exportMessageLst) {
    s3Con.getS3Connection((err, s3) => {
      if (!err) {
        uploadS3UnicaFile(exportDataValue, s3, emailCampaignJson, s3Location, exportMessageLst, (err) => {
          uploadS3SnkrsFile(exportDataValue, s3, snkrsLaunchJson, s3Location, exportMessageLst, (err) => {
            exportDataValue.exportStatus = "Completed"
            exportDataValue.exportMessage = exportMessageLst
            exportdata.upsert(exportDataValue).then(data => {
            }).catch(err => {
            })
          })
        })
      }
    })
  }

  // upload unica file to s3 location
  function uploadS3UnicaFile(exportDataValue, s3Conn, emailCampaignJson, s3Location, exportMessageLst, callback) {
    try {
      if (_.isEmpty(emailCampaignJson))
        return callback(null)
      const emailCampaignParams = {
        Bucket: s3Location,
        Key: exportDataValue.guid + '/email_campaign.json',
        Body: JSON.stringify(emailCampaignJson)
      };
      s3Conn.putObject(emailCampaignParams, function (err, data) {
        console.log("uploadS3UnicaFile" + err)
        console.log("uploadS3UnicaFile" + data)
        if (err)
          exportMessageLst.push({ field: 'email_campaign.json', value: 'Export Failed:' + err, timestamp: new Date() })
        else
          exportMessageLst.push({ field: 'email_campaign.json', value: 'Export Successful.', timestamp: new Date() })
        return callback(null)
      })
    } catch (err) {
      exportMessageLst.push({ field: 'email_campaign.json', value: 'Export Failed:' + err, timestamp: new Date() })
      return callback(null)
    }
  }

  // upload snkrs file to s3 location
  function uploadS3SnkrsFile(exportDataValue, s3Conn, snkrsLaunchJson, s3Location, exportMessageLst, callback) {
    try {
      if (_.isEmpty(snkrsLaunchJson))
        return callback(null)
      const snkrsCampaignParams = {
        Bucket: s3Location,
        Key: exportDataValue.guid + '/snkrs_campaign.json',
        Body: JSON.stringify(snkrsLaunchJson)
      };
      s3Conn.putObject(snkrsCampaignParams, function (err, data) {
        console.log("uploadS3SnkrsFile" + err)
        console.log("uploadS3SnkrsFile" + data)
        if (err)
          exportMessageLst.push({ field: 'snkrs_campaign.json', value: 'Export Failed:' + err, timestamp: new Date() })
        else
          exportMessageLst.push({ field: 'snkrs_campaign.json', value: 'Export Successful.', timestamp: new Date() })
        return callback(null)
      })
    } catch (err) {
      exportMessageLst.push({ field: 'snkrs_campaign.json', value: 'Export Failed:' + err, timestamp: new Date() })
      return callback(null)
    }
  }

  // upload data to box location
  function uploadDataToBox(exportDataValue, boxFolderId, emailCampaignJson, snkrsLaunchJson, exportMessageLst) {
    let userClient = boxCon.getBoxConnection();
    if (emailCampaignJson == null && snkrsLaunchJson == null) {
      exportDataValue.exportStatus = "Submitted"
      exportDataValue.exportMessage = exportMessageLst
      exportdata.upsert(exportDataValue).then(data => {
      }).catch(err => {
      })
    } else {
      getBoxFolderId(userClient, exportDataValue.folderId, boxFolderId, exportDataValue.guid, (err, folderId) => {
        if (err) {
          exportDataValue.exportStatus = "Submitted"
          exportDataValue.exportMessage = exportMessageLst
          exportdata.upsert(exportDataValue).then(() => {
          }).catch(err => { })
        } else {
          exportDataValue.folderId = folderId
          uploadUnicaFileForBox(userClient, exportDataValue, folderId, emailCampaignJson, exportMessageLst, (err, unicaFileId) => {
            exportDataValue.unicaFileBoxId = unicaFileId
            uploadSnkrsFileForBox(userClient, exportDataValue, folderId, snkrsLaunchJson, exportMessageLst, (err, snkrsFileId) => {
              exportDataValue.snkrsFileBoxId = snkrsFileId
              exportDataValue.exportStatus = "Submitted"
              exportDataValue.exportMessage = exportMessageLst
              exportdata.upsert(exportDataValue).then(data => {
              }).catch(err => { })
            })
          })
        }
      })
    }
  }

  //  create folder for guid in box location
  function getBoxFolderId(userClient, folderId, boxFolderId, guid, callback) {
    if (folderId == null) {
      userClient.folders.create(boxFolderId, guid).then(folder => {
        return callback(null, folder.id)
      }).catch(err => {
        return callback(err)
      })
    } else {
      return callback(null, folderId)
    }
  }

  // upload unica file to box location
  function uploadUnicaFileForBox(userClient, exportDataValue, folderId, emailCampaignJson, exportMessageLst, callback) {
    if (_.isEmpty(emailCampaignJson))
      return callback(null)
    if (exportDataValue.unicaFileBoxId == null) {
      userClient.files.uploadFile(folderId, 'email_campaign.json', JSON.stringify(emailCampaignJson)).then(file => {
        exportMessageLst.push({ field: "email_campaign.json", value: "Export Successful.", timestamp: new Date() })
        callback(null, file.entries[0].id)
      }).catch(err => {
        exportMessageLst.push({ field: "email_campaign.json", value: "Export Failed:" + err, timestamp: new Date() })
        callback(null, null)
      })
    } else {
      userClient.files.uploadNewFileVersion(exportDataValue.unicaFileBoxId, JSON.stringify(emailCampaignJson)).then(() => {
        exportMessageLst.push({ field: "email_campaign.json", value: "Export Successful.", timestamp: new Date() })
        callback(null, exportDataValue.unicaFileBoxId)
      }).catch(err => {
        exportMessageLst.push({ field: "email_campaign.json", value: "Export Failed:" + err, timestamp: new Date() })
        callback(null, null)
      })
    }
  }

  // upload snkrs file to box location
  function uploadSnkrsFileForBox(userClient, exportDataValue, folderId, snkrsLaunchJson, exportMessageLst, callback) {
    if (_.isEmpty(snkrsLaunchJson))
      return callback(null)
    if (exportDataValue.snkrsFileBoxId == null) {
      userClient.files.uploadFile(folderId, 'snkrsLaunch.json', JSON.stringify(snkrsLaunchJson)).then(file => {
        exportMessageLst.push({ field: "snkrsLaunch.json", value: "Export Successful.", timestamp: new Date() })
        callback(null, file.entries[0].id)
      }).catch(err => {
        exportMessageLst.push({ field: "snkrsLaunch.json", value: "Export Failed:" + err, timestamp: new Date() })
        callback(null, null)
      })
    } else {
      userClient.files.uploadNewFileVersion(exportDataValue.snkrsFileBoxId, JSON.stringify(snkrsLaunchJson)).then(() => {
        exportMessageLst.push({ field: "snkrsLaunch.json", value: "Export Successful.", timestamp: new Date() })
        callback(null, exportDataValue.snkrsFileBoxId)
      }).catch(err => {
        exportMessageLst.push({ field: "snkrsLaunch.json", value: "Export Failed:" + err, timestamp: new Date() })
        callback(null, null)
      })
    }
  }

  // get unica data from snowflake
  function getCampaignEmailData(emailId, adminData, callback) {
    let connection = snowflakeCon.getsnowFlakeConnection().connect()
    let result = []
    snowflakeCon.setSnowflakeRolePrevilage(connection, (err) => {
      if (err) {
        connection.destroy()
        return callback(err)
      }
      snowflakeCon.setSnowflakeWarehousePrevilage(connection, (err) => {
        if (err) {
          connection.destroy()
          return callback(err)
        }
        connection.execute({
          sqlText: 'select email_id,event_dttm from ' + adminData.exportUnicaEmailDatabase + "." + adminData.exportUnicaEmailSchema + "." + adminData.exportUnicaEmailTable + ' where lower(email_addr)= \'' + emailId + '\'',
          streamResult: true,
          complete: function (err, stmt, rows) {
            stmt.streamRows()
              .on('error', function (err) {
                connection.destroy()
                callback(err)
              })
              .on('data', function (row) {
                result.push(row.EVENT_DTTM)
              })
              .on('end', function () {
                connection.destroy()
                let emailCampaignJson = {
                  metadata: {
                    experience: "email_campaign",
                    version: "1.0.0",
                    src: "cde"
                  },
                  exportPayload: {}
                }
                if (!_.isEmpty(result)) {
                  emailCampaignJson.exportPayload = {
                    CampaignDates: result
                  }
                }
                callback(null, emailCampaignJson)
              });
          }
        })
      })
    })
  }

  // get snkrs data from snowflake
  function getSnkrsEmailData(upmid, adminData, callback) {
    let connection = snowflakeCon.getsnowFlakeConnection().connect()
    let result = []
    snowflakeCon.setSnowflakeRolePrevilage(connection, (err) => {
      if (err) {
        connection.destroy()
        return callback(err)
      }
      snowflakeCon.setSnowflakeWarehousePrevilage(connection, (err) => {
        if (err) {
          connection.destroy()
          return callback(err)
        }
        connection.execute({
          sqlText: 'select upmid,status,launchmethod,process_date_part from' + adminData.exportSnkrsLaunchDatabase + "." + adminData.exportSnkrsLaunchSchema + "." + adminData.exportSnkrsLaunchTable + ' where upmid= ' + upmid + " order by process_date_part desc",
          streamResult: true,
          complete: function (err, stmt, rows) {
            stmt.streamRows()
              .on('error', function (err) {
                connection.destroy()
                callback(err)
              })
              .on('data', function (row) {
                result.push({ UPMID: data.UPMID, STATUS: data.STATUS, LAUNCHMETHOD: data.LAUNCHMETHOD, PROCESSED_DATE_PART: data.PROCESS_DATE_PART })
              })
              .on('end', function () {
                connection.destroy()
                let emailCampaignJson = {
                  metadata: {
                    experience: "snkrsLaunch",
                    version: "1.0.0",
                    src: "cde"
                  },
                  exportPayload: {}
                }
                if (!_.isEmpty(result)) {
                  emailCampaignJson.exportPayload = {
                    CampaignDates: result
                  }
                }
                callback(null, emailCampaignJson)
              });
          }
        })
      })
    })
  }

  exportdata.remoteMethod('run', {
    http: { path: '/run', verb: 'get' },
    accepts: [{ arg: 'id', type: 'Number' },
    { arg: 'uploadType', type: 'String' },
    { arg: "req", type: "object", http: { source: "req" } }],
    returns: { arg: 'response', type: 'Object' }
  });

  function errorCallback(err, callback) {
    let error = new Error(err);
    error.status = 422;
    return callback(error)
  }

}