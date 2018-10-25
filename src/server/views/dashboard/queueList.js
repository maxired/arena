function handler(req, res) {
  const {Queues} = req.app.locals;
  const queues = Queues.list().then((queues) => {
    const basePath = req.baseUrl;

    return res.render('dashboard/templates/queueList', { basePath, queues });
  });

}

module.exports = handler;
