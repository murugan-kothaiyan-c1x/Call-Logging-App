module.exports = function (contact) {

    // validation check before creation of contact
    contact.beforeRemote('create', function (ctx, instance, next) {
        try {
            let phone = ctx.req.body.phone;
            let email = ctx.req.body.email;
            let re = /\S+@\S+\.\S+/;
            let mob = /^[1-9]{1}[0-9]{9}$/;
            if (mob.test(phone) == false) {
                return errorCallback('Phone number must be 10 digit', next)
            } else if (re.test(email) == false) {
                return errorCallback('Enter a valid email address', next)
            }
            next();
        } catch (err) {
            errorCallback(err, next)
        }
    })

    // validation check before updation of contact
    contact.beforeRemote('replaceById', function (ctx, instance, next) {
        try {
            let phone = ctx.req.body.phone;
            let email = ctx.req.body.email;
            let re = /\S+@\S+\.\S+/;
            let mob = /^[1-9]{1}[0-9]{9}$/;
            if (phone && mob.test(phone) == false) {
                return errorCallback('Phone number must be 10 digit', next)
            } else if (email && re.test(email) == false) {
                return errorCallback('Enter a valid email address', next)
            }
            next();
        } catch (err) {
            errorCallback(err, next)
        }
    })

    contact.validatesUniquenessOf('email', { message: 'email already exists' });
    contact.validatesUniquenessOf('name', { message: 'User name already exists' });
    // contact.validatesUniquenessOf('phone', { message: 'Phone Number already exists' });

    // contact.disableRemoteMethodByName("create", true);
    contact.disableRemoteMethodByName("upsert", true);
    contact.disableRemoteMethodByName("updateAll", true);
    contact.disableRemoteMethodByName("updateAttributes", true);

    // contact.disableRemoteMethodByName("find", true);
    // contact.disableRemoteMethodByName("findById", true);
    contact.disableRemoteMethodByName("findOne", true);

    // contact.disableRemoteMethodByName("deleteById", true);

    contact.disableRemoteMethodByName("confirm", true);
    contact.disableRemoteMethodByName("count", true);
    contact.disableRemoteMethodByName("exists", true);
    contact.disableRemoteMethodByName("resetPassword", true);

    // contact.disableRemoteMethodByName("update", true);
    contact.disableRemoteMethodByName("createChangeStream", true);
    contact.disableRemoteMethodByName("replaceOrCreate", true);
    // contact.disableRemoteMethodByName("replaceById", true);
    contact.disableRemoteMethodByName("upsertWithWhere", true);
    contact.disableRemoteMethodByName("patchOrCreate", true);
    contact.disableRemoteMethodByName("prototype.patchAttributes", true);

    function errorCallback(err, callback) {
        let error = new Error(err);
        error.status = 422;
        return callback(error)
    }

}