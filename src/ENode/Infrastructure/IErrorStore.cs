using System;
using System.Collections.Generic;
using System.Text;

namespace ENode.Infrastructure
{
    public interface IErrorStore
    {
        void SaveCommandError(string aggregateRootId,string command, string commandException, string messageId);
        void SaveEventError(string aggregateRootId, string eventException, int? version, string messageId);
    }
}

namespace ENode.Domain
{
    public class DomainBussinessException : Exception
    {
        public DomainBussinessException(string message) : base(message)
        {

        }
    }
}