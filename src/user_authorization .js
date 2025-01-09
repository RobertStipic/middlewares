export const userAuthorization  = (req, res, next) => {
    if(!req.currentUser){
        return res.status(401).send('Authorization failed');
    }

    next();
};