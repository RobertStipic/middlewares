export const adminAuthorization = (req, res, next) => {
  if (!req.currentUser) {
    return res.status(401).send("Authorization failed");
  }
  if (req.currentUser.role !== "admin") {
    return res.status(403).send("Admin access required");
  }
  next();
};
