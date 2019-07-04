let server = require('../../server/server');

module.exports = function (callLog) {

  // validation check and contactId check before creation of call log
  callLog.beforeRemote('create', function (ctx, instance, next) {
    try {
      let phoneParam = ctx.req.body.phoneNumber;
      let mob = /^[1-9]{1}[0-9]{9}$/;
      if (mob.test(phoneParam) == false) {
        return errorCallback('Phone number must be 10 digit', next)
      }
      server.models.contact.find({ where: { phone: phoneParam } }).then(contactValue => {
        ctx.req.body.contactId = contactValue[0].id
        next()
      })
    } catch (err) {
      errorCallback(err, next)
    }
  })

  // validation check for phonenumber and contact check before update
  callLog.beforeRemote('replaceById', function (ctx, instance, next) {
    try {
      let phone = ctx.req.body.phoneNumber;
      let mob = /^[1-9]{1}[0-9]{9}$/;
      if (phone && mob.test(phone) == false) {
        return errorCallback('Phone number must be 10 digit', next)
      }
      server.models.contact.find({ where: { phone: phoneParam } }).then(contactValue => {
        ctx.req.body.contactId = contactValue[0].id
        next()
      })
    } catch (err) {
      errorCallback(err, next)
    }
  })

  // callLog.disableRemoteMethodByName("create", true);
  callLog.disableRemoteMethodByName("upsert", true);
  callLog.disableRemoteMethodByName("updateAll", true);
  callLog.disableRemoteMethodByName("updateAttributes", true);

  // callLog.disableRemoteMethodByName("find", true);
  // callLog.disableRemoteMethodByName("findById", true);
  callLog.disableRemoteMethodByName("findOne", true);

  // callLog.disableRemoteMethodByName("deleteById", true);

  callLog.disableRemoteMethodByName("confirm", true);
  callLog.disableRemoteMethodByName("count", true);
  callLog.disableRemoteMethodByName("exists", true);
  callLog.disableRemoteMethodByName("resetPassword", true);

  // callLog.disableRemoteMethodByName("update", true);
  callLog.disableRemoteMethodByName("createChangeStream", true);
  callLog.disableRemoteMethodByName("replaceOrCreate", true);
  // callLog.disableRemoteMethodByName("replaceById", true);
  callLog.disableRemoteMethodByName("upsertWithWhere", true);
  callLog.disableRemoteMethodByName("patchOrCreate", true);
  callLog.disableRemoteMethodByName("prototype.patchAttributes", true);

  function errorCallback(err, callback) {
    let error = new Error(err);
    error.status = 422;
    return callback(error)
  }

}