let _ = require('lodash');
let request = require('request');
let moment = require('moment');
let server = require('../../server/server');
let creds = require('../../misc/credentials');
let DEBUG_MODE = true;

module.exports = function (deleteapi) {
  let pbdFrameworkApiURL = creds.PBD_API_FRAMEWORK_URL

  deleteapi.beforeRemote('deleteById', function (ctx, instance, next) {
    if (ctx.req.params != null && ctx.req.params.id != null) {
      deleteapi.findById(ctx.req.params.id).then(result => {
        if (_.isEmpty(result)) {
          return errorCallback('Invalid record', next)
        } else {
          next();
        }
      }).catch(err => {
        return errorCallback(err, next)
      })
    } else {
      return errorCallback(err, next)
    }
  });

  //Added empty string instead of null in data
  deleteapi.afterRemote('find', function (ctx, instance, next) {
    if (ctx.result != null) {
      ctx.result = _.map(ctx.result, (obj) => {
        return JSON.parse(JSON.stringify(obj).replace(/null/g, '""'))
      });
    }
    next()
  })

  // run add tag process with treatment id
  deleteapi.run = function (id, req, callback) {
    req.setTimeout(0);
    deleteapi.findById(id).then(deleteApiData => {
      try {
        if (_.isEmpty(deleteApiData))
          return errorCallback('Invalid data', callback)
        if (!_.isEqual(deleteApiData.status, 'ACTIVE'))
          return errorCallback('Invalid status.', callback)
        let catalogName = 'dtf_rel_delete_' + deleteApiData.type + '_' + moment().format('YYYYMMDDhhmmssSSS')
        console.log(catalogName)
        request.get(pbdFrameworkApiURL + 'AddRelease?catalog=' + catalogName, { timeout: 30000, json: false }, (err, response, body) => {
          if (DEBUG_MODE)
            console.log("Response ::", body);
          if (err)
            return errorCallback(err, callback)
          if (JSON.parse(body)['Error'] == null) {
            runAddDAG(deleteApiData, catalogName, (err, adminData) => {
              if (err)
                return errorCallback(err, callback)
              runDpaReactivationService(deleteApiData, adminData, catalogName, (err) => {
                if (err)
                  return errorCallback(err, callback)
                runStartEmr(deleteApiData, adminData, catalogName, (err) => {
                  if (err)
                    return errorCallback(err, callback)
                  runGdprAdobedeleteMain(deleteApiData, adminData, catalogName, (err) => {
                    if (err)
                      return errorCallback(err, callback)
                    runEmrTerminate(deleteApiData, catalogName, (err) => {
                      if (err)
                        return errorCallback(err, callback)
                      runSlack(deleteApiData, adminData, catalogName, (err) => {
                        if (err)
                          return errorCallback(err, callback)
                        createMKDir(adminData.bitbucketDagsRepo, catalogName, (err) => {
                          if (err)
                            return callback(null, [], err)
                          console.log('end of createMKDir')
                          runGitUserNameConfigDAG(adminData.bitbucketDagsRepoUser, adminData.bitbucketDagsRepo, catalogName, (err) => {
                            if (err)
                              return errorCallback(err, callback)
                            runGitEmailConfigDAG(adminData.bitbucketDagsRepoEmail, adminData.bitbucketDagsRepo, catalogName, (err) => {
                              if (err)
                                return errorCallback(err, callback)
                              runGitClone(adminData.bitbucketDagsRepo, adminData.bitbucketCloneUrl, catalogName, (err) => {
                                if (err)
                                  return errorCallback(err, callback)
                                if (DEBUG_MODE)
                                  console.log("compileRequest ::", pbdFrameworkApiURL + 'Compile?catalog=' + catalogName + "&target=" + adminData.bitbucketDagsRepo + '/' + catalogName + '/' + adminData.deleteDagsTargetFolder);
                                request.get(pbdFrameworkApiURL + 'Compile?catalog=' + catalogName + "&target=" + adminData.bitbucketDagsRepo + '/' + catalogName + '/' + adminData.deleteDagsTargetFolder, { timeout: 30000, json: false }, (error, response, compileResponse) => {
                                  if (DEBUG_MODE)
                                    console.log("compileResponse ::", compileResponse);
                                  if (error)
                                    return errorCallback(error, callback)
                                  if (!_.includes(compileResponse, '"Error":')) {
                                    runGitProcessDAG(adminData, catalogName, (err) => {
                                      if (err)
                                        return errorCallback(err, callback)
                                      callback(err, compileResponse)
                                    })
                                  } else {
                                    errorCallback(JSON.parse(compileResponse)['Error'], callback)
                                  }
                                })
                              })
                            })
                          })
                        })
                      })
                    })
                  })
                })
              })
            })
          } else {
            errorCallback(err == null ? err : JSON.parse(body)['Error'], callback)
          }
        })
      } catch (err) {
        errorCallback(err, callback)
      }
    }).catch(err => {
      errorCallback(err, callback)
    })
  }

  // run add DAG with treatment and admin data
  function runAddDAG(deleteApiData, catalogName, callback) {
    try {
      server.models.admin.find({ where: { status: { neq: null } } }).then(adminLst => {
        if (_.isEmpty(adminLst))
          return callback(null)
        let adminData = adminLst[0]
        let addDAGData = {
          Name: 'dtf_' + deleteApiData.type + '_delete',
          Catalog_name: catalogName,
          Default_args: adminData.adobeDefaultArgs
        }
        if (!_.isEmpty(adminData.adobeCustomCode))
          addDAGData.Code = adminData.adobeCustomCode
        if (DEBUG_MODE)
          console.log("Add DAG request ::", addDAGData)
        request.post({
          headers: { 'content-type': 'application/json' },
          url: pbdFrameworkApiURL + 'AddDAG',
          body: JSON.stringify(addDAGData),
          timeout: 30000
        }, (err, response, body) => {
          if (DEBUG_MODE)
            console.log("Add DAG response ::", body)
          if (err)
            return errorCallback(err, callback)
          if (JSON.parse(body)['Error'] == null) {
            callback(null, adminData)
          } else {
            errorCallback(JSON.parse(body)['Error'], callback)
          }
        })
      })
    } catch (err) {
      errorCallback(err, callback)
    }
  }

  // run runDpaReactivationService
  function runDpaReactivationService(deleteApiData, adminData, catalogName, callback) {
    let dpaDag = {
      DAG: 'GDPR_' + deleteApiData.type + '_DELETE_NOW',
      Catalog_name: catalogName,
      TaskType: 'TaskDependencySensor',
      TaskName: 'dpa_reactivation_service_chk',
      Dictionary: {
        task_id: "dpa_reactivation_service_chk",
        external_dag_id: adminData.adobeExternalDagId,
        external_task_id: adminData.adobeExternalTaskId,
        allowed_states: ["success"],
        cluster_id: "%!% profile['AIRFLOW_GCK_ENGINEERING_WG']",
        queue: 'airflow',
        dag: 'dag'
      },
      AfterCode: [

      ]
    }
    if (DEBUG_MODE)
      console.log("runDpaReactivationService request ::", dpaDag)
    request.post({
      headers: { 'content-type': 'application/json' },
      url: pbdFrameworkApiURL + 'AddTask',
      body: JSON.stringify(dpaDag),
      timeout: 30000
    }, (err, response, body) => {
      if (DEBUG_MODE)
        console.log("runDpaReactivationService response::", body)
      if (err)
        return errorCallback(err, callback)
      if (JSON.parse(body)['Error'] == null) {
        callback(null)
      } else {
        errorCallback(JSON.parse(body)['Error'], callback)
      }
    })
  }

  function runStartEmr(deleteApiData, adminData, catalogName, callback) {
    let startEmrDag = {
      DAG: 'GDPR_' + deleteApiData.type + '_DELETE_NOW',
      Catalog_name: catalogName,
      TaskType: 'EmrOperator',
      TaskName: 'StartEMR',
      Upstream: ['dpa_reactivation_service_chk'],
      'Dictionary': {
        task_id: 'Emr_Spinup',
        cluster_action: 'spinup',
        cluster_name: '%!% CLUSTER_NM',
        emr_version: '5.11.0',
        num_core_nodes: adminData.adobeVnumCoreNodes,
        core_inst_type: adminData.adobeVcoreInstType,
        num_task_nodes: adminData.adobeVnumTaskNodes,
        task_inst_type: adminData.adobeVtaskInstType,
        task_bid_type: 'ON_DEMAND',
        applications: '%!% APPLICATIONS',
        queue: 'airflow',
        classification: 'silver',
        project_id: 'FY150038-CK',
        bootstrap_actions: '%!% BOOTSTRAP_ACTIONS',
        dag: 'dag'
      },
      "AfterCode": [

      ]
    }
    if (DEBUG_MODE)
      console.log("start emr request ::", startEmrDag)
    request.post({
      headers: { 'content-type': 'application/json' },
      url: pbdFrameworkApiURL + 'AddTask',
      body: JSON.stringify(startEmrDag),
      timeout: 30000
    }, (err, response, body) => {
      if (DEBUG_MODE)
        console.log("start emr response::", body)
      if (err)
        return errorCallback(err, callback)
      if (JSON.parse(body)['Error'] == null) {
        callback(null)
      } else {
        errorCallback(JSON.parse(body)['Error'], callback)
      }
    })
  }

  function runGdprAdobedeleteMain(deleteApiData, adminData, catalogName, callback) {
    let gdprDag = {
      DAG: 'GDPR_' + deleteApiData.type + '_DELETE_NOW',
      Catalog_name: catalogName,
      TaskType: 'GenieSparkOperator',
      TaskName: 'GDPR_Adobe_Delete_Main',
      Upstream: ['StartEMR'],
      Dictionary: {
        task_id: 'gdpr_adobe_delete_main',
        command: adminData.adobeSparkCommand,
        job_name: 'gdpr_adobe_delete_main_dev',
        sched_type: '%!% CLUSTER_NM',
        queue: 'airflow',
        dag: 'dag'
      }
    }
    if (DEBUG_MODE)
      console.log("GDPR Delete main request ::", gdprDag)
    request.post({
      headers: { 'content-type': 'application/json' },
      url: pbdFrameworkApiURL + 'AddTask',
      body: JSON.stringify(gdprDag),
      timeout: 30000
    }, (err, response, body) => {
      if (DEBUG_MODE)
        console.log("GDPR Delete main response::", body)
      if (err)
        return errorCallback(err, callback)
      if (JSON.parse(body)['Error'] == null) {
        callback(null)
      } else {
        errorCallback(JSON.parse(body)['Error'], callback)
      }
    })
  }

  function runEmrTerminate(deleteApiData, catalogName, callback) {
    let emrTerminateDag = {
      DAG: 'GDPR_' + deleteApiData.type + '_DELETE_NOW',
      Catalog_name: catalogName,
      TaskType: 'EmrOperator',
      TaskName: 'EmrTerminate',
      Upstream: ['GDPR_Adobe_Delete_Main'],
      Dictionary: {
        task_id: 'Emr_Terminate',
        cluster_action: 'terminate',
        cluster_name: '%!% CLUSTER_NM',
        queue: 'airflow',
        dag: 'dag'
      }
    }
    if (DEBUG_MODE)
      console.log("emr terminate request ::", emrTerminateDag)
    request.post({
      headers: { 'content-type': 'application/json' },
      url: pbdFrameworkApiURL + 'AddTask',
      body: JSON.stringify(emrTerminateDag),
      timeout: 30000
    }, (err, response, body) => {
      if (DEBUG_MODE)
        console.log("emr terminate response::", body)
      if (err)
        return errorCallback(err, callback)
      if (JSON.parse(body)['Error'] == null) {
        callback(null)
      } else {
        errorCallback(JSON.parse(body)['Error'], callback)
      }
    })
  }

  function runSlack(deleteApiData, adminData, catalogName, callback) {
    let slackDag = {
      DAG: 'GDPR_' + deleteApiData.type + '_DELETE_NOW',
      Catalog_name: catalogName,
      TaskType: 'SlackOperator',
      TaskName: 'Slack',
      Upstream: ['EmrTerminate'],
      Dictionary: {
        task_id: 'send_slack',
        channel: adminData.adobeSlackChannel,
        message: adminData.adobeSlackMessage,
        dag: 'dag'
      }
    }
    if (DEBUG_MODE)
      console.log("slack request ::", slackDag)
    request.post({
      headers: { 'content-type': 'application/json' },
      url: pbdFrameworkApiURL + 'AddTask',
      body: JSON.stringify(slackDag),
      timeout: 30000
    }, (err, response, body) => {
      if (DEBUG_MODE)
        console.log("slack response::", body)
      if (err)
        return errorCallback(err, callback)
      if (JSON.parse(body)['Error'] == null) {
        callback(null)
      } else {
        errorCallback(JSON.parse(body)['Error'], callback)
      }
    })
  }

  function runGitUserNameConfigDAG(userName, repoPath, catalogName, callback) {
    try {
      let nameConfig = {
        Directory: repoPath + '/' + catalogName,
        Args: [
          "config",
          "--global",
          "user.name",
          userName
        ]
      }
      if (DEBUG_MODE)
        console.log("user name config request ::", nameConfig)
      request.post({
        headers: { 'content-type': 'application/json' },
        url: pbdFrameworkApiURL + 'Git',
        body: JSON.stringify(nameConfig),
        timeout: 30000
      }, (err, response, body) => {
        if (DEBUG_MODE)
          console.log("user name config post ::", body)
        if (err)
          return errorCallback(err, callback)
        if (body != null && _.includes(body, '"Error":')) {
          errorCallback(body, callback)
        } else {
          callback(null)
        }
      })
    } catch (err) {
      errorCallback(err, callback)
    }
  }

  function runGitEmailConfigDAG(email, repoPath, catalogName, callback) {
    try {
      let emailConfig = {
        Directory: repoPath + '/' + catalogName,
        Args: [
          "config",
          "--global",
          "user.email",
          email
        ]
      }
      if (DEBUG_MODE)
        console.log("email config request ::", emailConfig);
      request.post({
        headers: { 'content-type': 'application/json' },
        url: pbdFrameworkApiURL + 'Git',
        body: JSON.stringify(emailConfig),
        timeout: 30000
      }, (err, response, body) => {
        if (DEBUG_MODE)
          console.log("email config response ::", body);
        if (err)
          return errorCallback(err, callback)
        if (body != null && _.includes(body, '"Error":')) {
          errorCallback(body, callback)
        } else {
          callback(null)
        }
      })
    } catch (err) {
      errorCallback(err, callback)
    }
  }

  function createMKDir(repoPath, catalogName, callback) {
    let mkdirValue = {
      Path: repoPath + '/' + catalogName,
      Command: "mkdir"
    }
    if (DEBUG_MODE)
      console.log("mkdir request ::", JSON.stringify(mkdirValue))
    request.post({
      headers: { 'content-type': 'application/json' },
      url: pbdFrameworkApiURL + 'Files',
      body: JSON.stringify(mkdirValue),
      timeout: 30000
    }, (error, response, body) => {
      if (DEBUG_MODE)
        console.log("mkdir response ::", body)
      if (error)
        return errorCallback(error, callback)
      callback(null)
    })
  }

  function runGitClone(repoPath, bitbucketCloneUrl, catalogName, callback) {
    let mkdirValue = {
      Directory: repoPath + '/' + catalogName,
      Args: [
        "clone",
        bitbucketCloneUrl
      ]
    }
    if (DEBUG_MODE)
      console.log("git clone request ::", JSON.stringify(mkdirValue))
    request.post({
      headers: { 'content-type': 'application/json' },
      url: pbdFrameworkApiURL + 'Git',
      body: JSON.stringify(mkdirValue),
      timeout: 30000
    }, (error, response, body) => {
      if (DEBUG_MODE)
        console.log("git clone response ::", body)
      if (error)
        return errorCallback(error, callback)
      callback(null)
    })
  }

  //  run git process DAG
  function runGitProcessDAG(adminData, catalogName, callback) {
    runGitBranchCheckoutDAG(adminData.bitbucketDagsRepo, adminData.deleteDagsTargetFolder, catalogName, (err) => {
      if (err)
        return callback(err)
      runGitAddDAG(adminData.bitbucketDagsRepo, adminData.deleteDagsTargetFolder, catalogName, (err) => {
        if (err)
          return callback(err)
        runGitCommitDAG(adminData.bitbucketDagsRepo, adminData.deleteDagsTargetFolder, catalogName, (err) => {
          if (err)
            return callback(err)
          runGitPushDAG(adminData.bitbucketDagsRepo, adminData.deleteDagsTargetFolder, catalogName, branchName, (err) => {
            return callback(err)
          })
        })
      })
    })
  }

  function runGitBranchCheckoutDAG(repoPath, deleteDagsTargetFolder, catalogName, callback) {
    try {
      let gitBranchCheckout = {
        Directory: repoPath + '/' + catalogName + '/' + deleteDagsTargetFolder,
        Args: [
          "checkout",
          "-b",
          catalogName
        ]
      }
      if (DEBUG_MODE)
        console.log("Git branch checkout post ::", gitBranchCheckout);
      request.post({
        headers: { 'content-type': 'application/json' },
        url: pbdFrameworkApiURL + 'Git',
        body: JSON.stringify(gitBranchCheckout),
        timeout: 30000
      }, (err, response, body) => {
        if (DEBUG_MODE)
          console.log("Git branch checkout response ::", body);
        if (err)
          return errorCallback(err, callback)
        if (body != null && _.includes(body, '"Error":')) {
          errorCallback(body, callback)
        } else {
          callback(null)
        }
      })
    } catch (err) {
      errorCallback(err, callback)
    }
  }



  function runGitAddDAG(repoPath, deleteDagsTargetFolder, catalogName, callback) {
    try {
      let gitAddConfig = {
        Directory: repoPath + '/' + catalogName + '/' + deleteDagsTargetFolder,
        Args: [
          "add",
          "."
        ]
      }
      if (DEBUG_MODE)
        console.log("Post ::", gitAddConfig);
      request.post({
        headers: { 'content-type': 'application/json' },
        url: pbdFrameworkApiURL + 'Git',
        body: JSON.stringify(gitAddConfig),
        timeout: 30000
      }, (err, response, body) => {
        if (DEBUG_MODE)
          console.log("Response ::", body);
        if (err)
          return errorCallback(err, callback)
        if (body != null && _.includes(body, '"Error":')) {
          errorCallback(body, callback)
        } else {
          callback(null)
        }
      })
    } catch (err) {
      errorCallback(err, callback)
    }
  }

  function runGitCommitDAG(repoPath, deleteDagsTargetFolder, catalogName, callback) {
    try {
      let gitCommitConfig = {
        Directory: repoPath + '/' + catalogName + '/' + deleteDagsTargetFolder,
        Args: [
          "commit",
          "-m",
          catalogName
        ]
      }
      if (DEBUG_MODE)
        console.log("Git commit Post ::", gitCommitConfig);
      request.post({
        headers: { 'content-type': 'application/json' },
        url: pbdFrameworkApiURL + 'Git',
        body: JSON.stringify(gitCommitConfig),
        timeout: 30000
      }, (err, response, body) => {
        if (DEBUG_MODE)
          console.log("Git commit Response ::", body);
        if (err)
          return errorCallback(err, callback)
        if (body != null && _.includes(body, '"Error":')) {
          errorCallback(body, callback)
        } else {
          callback(null)
        }
      })
    } catch (err) {
      errorCallback(err, callback)
    }
  }

  function runGitPushDAG(repoPath, deleteDagsTargetFolder, catalogName, branchName, callback) {
    try {
      let gitPushConfig = {
        Directory: repoPath + '/' + catalogName + '/' + deleteDagsTargetFolder,
        Args: [
          "push",
          "origin",
          branchName
        ]
      }
      if (DEBUG_MODE)
        console.log("Git push post ::", gitPushConfig);
      request.post({
        headers: { 'content-type': 'application/json' },
        url: pbdFrameworkApiURL + 'Git',
        body: JSON.stringify(gitPushConfig),
        timeout: 30000
      }, (err, response, body) => {
        if (DEBUG_MODE)
          console.log("GIT push response ::", body);
        if (err)
          return errorCallback(err, callback)
        if (body != null && _.includes(body, '"Error":')) {
          errorCallback(body, callback)
        } else {
          callback(null)
        }
      })
    } catch (err) {
      errorCallback(err, callback)
    }
  }


  deleteapi.remoteMethod('run', {
    http: { path: '/run', verb: 'get' },
    accepts: [{ arg: 'id', type: 'Number' },
    { arg: "req", type: "object", http: { source: "req" } }],
    returns: [{ arg: 'response', type: 'Object' }]
  });

  function errorCallback(err, callback) {
    try {
      let error = new Error(err);
      error.status = 422;
      return callback(error)
    } catch (error) {
      let errorObj = new Error(error);
      errorObj.status = 422;
      return callback(errorObj)
    }
  }
}
